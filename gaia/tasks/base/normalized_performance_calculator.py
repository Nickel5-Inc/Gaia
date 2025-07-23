import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass
import json

import numpy as np
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from gaia.database.validator_schema import (
    # New normalized tables
    miner_performance_summary_table,
    task_performance_metrics_table,
    weather_performance_phases_table,
    miner_weight_calculations_table,
    miner_consensus_data_table,
    # Data source tables
    weather_miner_scores_table,
    weather_miner_responses_table, 
    weather_forecast_runs_table,
    weather_score_phases_table,
    soil_moisture_history_table,
    geomagnetic_history_table,
    node_table
)

logger = logging.getLogger(__name__)

@dataclass
class NormalizedTaskMetrics:
    """Task metrics for normalized storage."""
    task_name: str
    attempted: int = 0
    completed: int = 0
    scored: int = 0
    avg_score: Optional[float] = None
    success_rate: Optional[float] = None
    best_score: Optional[float] = None
    latest_score: Optional[float] = None
    rank: Optional[int] = None
    percentile_rank: Optional[float] = None
    weight_contribution: Optional[float] = None
    task_specific_metrics: Dict[str, Any] = None

@dataclass
class WeatherPhaseMetrics:
    """Weather phase-specific metrics."""
    phase_type: str  # 'initial' or 'final'
    runs_with_phase_scores: int = 0
    avg_phase_score: Optional[float] = None
    best_phase_score: Optional[float] = None
    latest_phase_score: Optional[float] = None
    phase_rank: Optional[int] = None
    avg_scoring_lag_days: Optional[float] = None
    data_availability_summary: Dict[str, Any] = None

@dataclass
class NormalizedMinerPerformance:
    """Normalized performance profile for a miner."""
    miner_uid: str
    miner_hotkey: str
    period_start: datetime
    period_end: datetime
    period_type: str
    
    # Summary metrics
    total_attempted: int = 0
    total_completed: int = 0
    overall_success_rate: Optional[float] = None
    overall_avg_score: Optional[float] = None
    overall_rank: Optional[int] = None
    performance_trend: Optional[str] = None
    trend_confidence: Optional[float] = None
    last_active_time: Optional[datetime] = None
    consecutive_failures: int = 0
    uptime_percentage: Optional[float] = None
    
    # Task-specific metrics
    task_metrics: List[NormalizedTaskMetrics] = None
    
    # Weather phase metrics
    weather_phases: List[WeatherPhaseMetrics] = None
    
    # Weight calculation data
    submitted_weight: Optional[float] = None
    raw_calculated_weight: Optional[float] = None
    excellence_weight: Optional[float] = None
    diversity_weight: Optional[float] = None
    scoring_pathway: Optional[str] = None
    pathway_details: Optional[Dict[str, Any]] = None
    multi_task_bonus: Optional[float] = None
    
    # Consensus data
    incentive: Optional[float] = None
    consensus_rank: Optional[int] = None
    weight_submission_block: Optional[int] = None
    consensus_block: Optional[int] = None

class NormalizedMinerPerformanceCalculator:
    """
    Performance calculator that writes to normalized tables.
    
    This calculator implements the same logic as the original but stores
    data in the new normalized schema for better maintainability.
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self._current_pathway_data = None
        self._current_consensus_data = None

    async def calculate_and_store_period_stats(
        self,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
        miner_uids: Optional[List[str]] = None,
        pathway_data: Optional[Dict] = None,
        consensus_data: Optional[Dict] = None
    ) -> List[NormalizedMinerPerformance]:
        """
        Calculate performance stats and store them in normalized tables.
        
        This is the main entry point that replaces the old monolithic approach.
        """
        logger.info(f"Calculating normalized performance stats for period {period_type}: {period_start} to {period_end}")
        
        # Store pathway and consensus data for integration
        self._current_pathway_data = pathway_data
        self._current_consensus_data = consensus_data
        
        # Get active miners if not provided
        if miner_uids is None:
            miner_uids = await self._get_active_miners(period_start, period_end)
            
        if not miner_uids:
            logger.warning(f"No active miners found for period {period_type}")
            return []
            
        performances = []
        
        for miner_uid in miner_uids:
            try:
                performance = await self._calculate_miner_performance(
                    miner_uid, period_start, period_end, period_type
                )
                if performance:
                    performances.append(performance)
                    
            except Exception as e:
                logger.error(f"Error calculating performance for miner {miner_uid}: {e}")
                continue
        
        # Calculate cross-miner rankings
        self._calculate_rankings(performances)
        
        # Store in normalized tables
        await self._store_normalized_performance(performances)
        
        logger.info(f"Calculated and stored normalized performance for {len(performances)} miners")
        return performances

    async def _calculate_miner_performance(
        self, miner_uid: str, period_start: datetime, period_end: datetime, period_type: str
    ) -> Optional[NormalizedMinerPerformance]:
        """Calculate performance for a single miner."""
        
        # Get miner hotkey
        miner_hotkey = await self._get_miner_hotkey(miner_uid)
        if not miner_hotkey:
            logger.warning(f"No hotkey found for miner UID {miner_uid}")
            return None
            
        performance = NormalizedMinerPerformance(
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            period_start=period_start,
            period_end=period_end,
            period_type=period_type
        )
        
        # Calculate task-specific metrics
        performance.task_metrics = []
        
        # Weather metrics (with phase support)
        weather_metrics = await self._calculate_weather_metrics_normalized(
            miner_uid, miner_hotkey, period_start, period_end
        )
        if weather_metrics:
            performance.task_metrics.append(weather_metrics)
            
        # Weather phase metrics
        performance.weather_phases = await self._calculate_weather_phase_metrics(
            miner_uid, miner_hotkey, period_start, period_end
        )
        
        # Soil moisture metrics
        soil_metrics = await self._calculate_soil_metrics_normalized(
            miner_uid, miner_hotkey, period_start, period_end
        )
        if soil_metrics:
            performance.task_metrics.append(soil_metrics)
            
        # Geomagnetic metrics
        geo_metrics = await self._calculate_geomagnetic_metrics_normalized(
            miner_uid, miner_hotkey, period_start, period_end
        )
        if geo_metrics:
            performance.task_metrics.append(geo_metrics)
            
        # Calculate overall metrics from task metrics
        self._calculate_overall_metrics(performance)
        
        # Integrate pathway and consensus data
        self._integrate_external_data(performance, miner_uid)
        
        return performance

    async def _calculate_weather_metrics_normalized(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> Optional[NormalizedTaskMetrics]:
        """
        Calculate weather metrics with integration of weather_score_phases data.
        
        This method demonstrates the integration with our new phase-aware scoring.
        """
        metrics = NormalizedTaskMetrics(
            task_name="weather",
            task_specific_metrics={}
        )
        
        # Get basic response data
        response_query = sa.select(
            weather_miner_responses_table.c.status,
            weather_forecast_runs_table.c.run_initiation_time,
        ).select_from(
            weather_miner_responses_table.join(weather_forecast_runs_table)
        ).where(
            sa.and_(
                weather_miner_responses_table.c.miner_uid == int(miner_uid),
                weather_forecast_runs_table.c.run_initiation_time.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(response_query)
        response_rows = result.fetchall()
        
        if not response_rows:
            return None
            
        # Calculate basic metrics
        metrics.attempted = len(response_rows)
        completed_statuses = ['completed', 'verified', 'scored']
        metrics.completed = sum(1 for row in response_rows if row.status in completed_statuses)
        
        if metrics.attempted > 0:
            metrics.success_rate = metrics.completed / metrics.attempted
            
        # Get scoring data from both old weather_miner_scores and new weather_score_phases
        
        # 1. Traditional scores (for compatibility)
        traditional_scores = await self._get_traditional_weather_scores(
            miner_uid, start, end
        )
        
        # 2. NEW: Phase-aware scores from weather_score_phases
        phase_scores = await self._get_weather_phase_scores(
            miner_uid, start, end
        )
        
        # Combine scoring data
        all_scores = []
        if traditional_scores:
            all_scores.extend(traditional_scores)
        if phase_scores:
            all_scores.extend(phase_scores)
            
        if all_scores:
            metrics.scored = len(all_scores)
            metrics.avg_score = float(np.mean(all_scores))
            metrics.best_score = float(np.max(all_scores))
            metrics.latest_score = float(all_scores[-1])  # Assume sorted by time
            
        # Store detailed weather metrics
        metrics.task_specific_metrics = {
            'total_forecasts': metrics.attempted,
            'traditional_scores_count': len(traditional_scores) if traditional_scores else 0,
            'phase_scores_count': len(phase_scores) if phase_scores else 0,
            'scoring_method': 'phase_aware' if phase_scores else 'traditional'
        }
        
        return metrics

    async def _get_traditional_weather_scores(
        self, miner_uid: str, start: datetime, end: datetime
    ) -> List[float]:
        """Get scores from traditional weather_miner_scores table."""
        query = sa.select(
            weather_miner_scores_table.c.score
        ).select_from(
            weather_miner_scores_table
            .join(weather_miner_responses_table)
            .join(weather_forecast_runs_table)
        ).where(
            sa.and_(
                weather_miner_responses_table.c.miner_uid == int(miner_uid),
                weather_forecast_runs_table.c.run_initiation_time.between(start, end),
                weather_miner_scores_table.c.score.isnot(None)
            )
        ).order_by(weather_miner_scores_table.c.calculation_time)
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        return [float(row.score) for row in rows if row.score is not None]

    async def _get_weather_phase_scores(
        self, miner_uid: str, start: datetime, end: datetime
    ) -> List[float]:
        """Get scores from new weather_score_phases table."""
        query = sa.select(
            weather_score_phases_table.c.score_value
        ).select_from(
            weather_score_phases_table
            .join(weather_forecast_runs_table)
        ).where(
            sa.and_(
                weather_score_phases_table.c.miner_uid == int(miner_uid),
                weather_forecast_runs_table.c.run_initiation_time.between(start, end),
                weather_score_phases_table.c.score_value.isnot(None)
            )
        ).order_by(weather_score_phases_table.c.calculation_time)
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        return [float(row.score_value) for row in rows if row.score_value is not None]

    async def _calculate_weather_phase_metrics(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> List[WeatherPhaseMetrics]:
        """
        Calculate weather phase-specific metrics from weather_score_phases table.
        
        This is the key integration with our new dual-phase scoring system.
        """
        phase_metrics = []
        
        for phase_type in ['initial', 'final']:
            query = sa.select(
                weather_score_phases_table.c.score_value,
                weather_score_phases_table.c.calculation_time,
                weather_score_phases_table.c.data_availability,
                weather_forecast_runs_table.c.run_initiation_time
            ).select_from(
                weather_score_phases_table
                .join(weather_forecast_runs_table)
            ).where(
                sa.and_(
                    weather_score_phases_table.c.miner_uid == int(miner_uid),
                    weather_score_phases_table.c.phase_type == phase_type,
                    weather_forecast_runs_table.c.run_initiation_time.between(start, end),
                    weather_score_phases_table.c.score_value.isnot(None)
                )
            ).order_by(weather_score_phases_table.c.calculation_time)
            
            result = await self.db_manager.execute(query)
            rows = result.fetchall()
            
            if rows:
                scores = [float(row.score_value) for row in rows]
                
                # Calculate scoring lag
                lag_times = []
                for row in rows:
                    if row.calculation_time and row.run_initiation_time:
                        lag_days = (row.calculation_time - row.run_initiation_time).days
                        lag_times.append(lag_days)
                
                # Aggregate data availability
                data_availability = {}
                for row in rows:
                    if row.data_availability:
                        try:
                            availability = json.loads(row.data_availability) if isinstance(row.data_availability, str) else row.data_availability
                            for key, value in availability.items():
                                if key not in data_availability:
                                    data_availability[key] = []
                                data_availability[key].append(value)
                        except (json.JSONDecodeError, TypeError):
                            continue
                
                phase_metrics.append(WeatherPhaseMetrics(
                    phase_type=phase_type,
                    runs_with_phase_scores=len(scores),
                    avg_phase_score=float(np.mean(scores)),
                    best_phase_score=float(np.max(scores)),
                    latest_phase_score=float(scores[-1]) if scores else None,
                    avg_scoring_lag_days=float(np.mean(lag_times)) if lag_times else None,
                    data_availability_summary=data_availability
                ))
        
        return phase_metrics

    async def _calculate_soil_metrics_normalized(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> Optional[NormalizedTaskMetrics]:
        """Calculate normalized soil moisture metrics."""
        query = sa.select(
            soil_moisture_history_table.c.surface_rmse,
            soil_moisture_history_table.c.rootzone_rmse,
            soil_moisture_history_table.c.surface_structure_score,
            soil_moisture_history_table.c.rootzone_structure_score,
        ).where(
            sa.and_(
                soil_moisture_history_table.c.miner_uid == miner_uid,
                soil_moisture_history_table.c.scored_at.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        
        if not rows:
            return None
            
        # Calculate combined scores
        combined_scores = []
        surface_rmses = []
        rootzone_rmses = []
        
        for row in rows:
            if row.surface_structure_score is not None and row.rootzone_structure_score is not None:
                combined_scores.append((row.surface_structure_score + row.rootzone_structure_score) / 2.0)
            if row.surface_rmse is not None:
                surface_rmses.append(row.surface_rmse)
            if row.rootzone_rmse is not None:
                rootzone_rmses.append(row.rootzone_rmse)
                
        metrics = NormalizedTaskMetrics(
            task_name="soil_moisture",
            attempted=len(rows),
            completed=len(rows),
            scored=len(combined_scores),
            success_rate=1.0,
            task_specific_metrics={
                'avg_surface_rmse': float(np.mean(surface_rmses)) if surface_rmses else None,
                'avg_rootzone_rmse': float(np.mean(rootzone_rmses)) if rootzone_rmses else None,
                'total_predictions': len(rows)
            }
        )
        
        if combined_scores:
            metrics.avg_score = float(np.mean(combined_scores))
            metrics.best_score = float(np.max(combined_scores))
            metrics.latest_score = float(combined_scores[-1])
            
        return metrics

    async def _calculate_geomagnetic_metrics_normalized(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> Optional[NormalizedTaskMetrics]:
        """Calculate normalized geomagnetic metrics."""
        query = sa.select(
            geomagnetic_history_table.c.score,
            geomagnetic_history_table.c.predicted_value,
            geomagnetic_history_table.c.ground_truth_value,
        ).where(
            sa.and_(
                geomagnetic_history_table.c.miner_uid == miner_uid,
                geomagnetic_history_table.c.query_time.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        
        if not rows:
            return None
            
        scores = [float(row.score) for row in rows if row.score is not None]
        
        # Calculate prediction errors
        errors = []
        for row in rows:
            if row.predicted_value is not None and row.ground_truth_value is not None:
                errors.append(abs(row.predicted_value - row.ground_truth_value))
                
        metrics = NormalizedTaskMetrics(
            task_name="geomagnetic",
            attempted=len(rows),
            completed=len(rows),
            scored=len(scores),
            success_rate=1.0,
            task_specific_metrics={
                'avg_prediction_error': float(np.mean(errors)) if errors else None,
                'total_predictions': len(rows)
            }
        )
        
        if scores:
            metrics.avg_score = float(np.mean(scores))
            metrics.best_score = float(np.max(scores))
            metrics.latest_score = float(scores[-1])
            
        return metrics

    def _calculate_overall_metrics(self, performance: NormalizedMinerPerformance):
        """Calculate overall aggregated metrics from task metrics."""
        if not performance.task_metrics:
            return
            
        # Sum up totals
        performance.total_attempted = sum(tm.attempted for tm in performance.task_metrics)
        performance.total_completed = sum(tm.completed for tm in performance.task_metrics)
        
        if performance.total_attempted > 0:
            performance.overall_success_rate = performance.total_completed / performance.total_attempted
            
        # Calculate weighted average score
        task_scores = []
        task_weights = []
        
        for tm in performance.task_metrics:
            if tm.avg_score is not None and tm.scored > 0:
                task_scores.append(tm.avg_score)
                task_weights.append(tm.scored)
                
        if task_scores:
            scores_array = np.array(task_scores, dtype=np.float64)
            weights_array = np.array(task_weights, dtype=np.float64)
            performance.overall_avg_score = float(np.average(scores_array, weights=weights_array))

    def _integrate_external_data(self, performance: NormalizedMinerPerformance, miner_uid: str):
        """Integrate pathway and consensus data."""
        try:
            # Integrate pathway data
            if (self._current_pathway_data and 
                int(miner_uid) in self._current_pathway_data):
                
                pathway_data = self._current_pathway_data[int(miner_uid)]
                performance.submitted_weight = pathway_data.get('submitted_weight')
                performance.raw_calculated_weight = pathway_data.get('raw_calculated_weight')
                performance.excellence_weight = pathway_data.get('excellence_weight')
                performance.diversity_weight = pathway_data.get('diversity_weight')
                performance.scoring_pathway = pathway_data.get('scoring_pathway')
                performance.pathway_details = pathway_data.get('pathway_details')
                performance.multi_task_bonus = pathway_data.get('multi_task_bonus')
                
            # Integrate consensus data
            if (self._current_consensus_data and 
                int(miner_uid) in self._current_consensus_data):
                
                consensus_data = self._current_consensus_data[int(miner_uid)]
                performance.incentive = consensus_data.get('incentive')
                performance.consensus_rank = consensus_data.get('consensus_rank')
                performance.consensus_block = consensus_data.get('consensus_block')
                
        except Exception as e:
            logger.warning(f"Error integrating external data for miner {miner_uid}: {e}")

    def _calculate_rankings(self, performances: List[NormalizedMinerPerformance]):
        """Calculate rankings across all miners."""
        if not performances:
            return
            
        # Overall ranking
        scored_performances = [p for p in performances if p.overall_avg_score is not None]
        if scored_performances:
            scores = np.array([p.overall_avg_score for p in scored_performances])
            rank_indices = np.argsort(-scores)  # Descending order
            ranks = np.empty_like(rank_indices)
            ranks[rank_indices] = np.arange(1, len(rank_indices) + 1)
            
            for i, performance in enumerate(scored_performances):
                performance.overall_rank = int(ranks[i])
                
        # Task-specific rankings
        task_names = ['weather', 'soil_moisture', 'geomagnetic']
        for task_name in task_names:
            task_performances = []
            task_metrics_list = []
            
            for performance in performances:
                for tm in (performance.task_metrics or []):
                    if tm.task_name == task_name and tm.avg_score is not None:
                        task_performances.append(performance)
                        task_metrics_list.append(tm)
                        break
                        
            if task_metrics_list:
                task_scores = np.array([tm.avg_score for tm in task_metrics_list])
                rank_indices = np.argsort(-task_scores)
                ranks = np.empty_like(rank_indices)
                ranks[rank_indices] = np.arange(1, len(rank_indices) + 1)
                
                for i, tm in enumerate(task_metrics_list):
                    tm.rank = int(ranks[i])
                    
        # Weather phase rankings
        for phase_type in ['initial', 'final']:
            phase_performances = []
            phase_metrics_list = []
            
            for performance in performances:
                for wpm in (performance.weather_phases or []):
                    if wpm.phase_type == phase_type and wpm.avg_phase_score is not None:
                        phase_performances.append(performance)
                        phase_metrics_list.append(wpm)
                        break
                        
            if phase_metrics_list:
                phase_scores = np.array([wpm.avg_phase_score for wpm in phase_metrics_list])
                rank_indices = np.argsort(-phase_scores)
                ranks = np.empty_like(rank_indices)
                ranks[rank_indices] = np.arange(1, len(rank_indices) + 1)
                
                for i, wpm in enumerate(phase_metrics_list):
                    wpm.phase_rank = int(ranks[i])

    async def _store_normalized_performance(self, performances: List[NormalizedMinerPerformance]):
        """Store performance data in normalized tables."""
        if not performances:
            return
            
        logger.info(f"Storing normalized performance data for {len(performances)} miners")
        
        # Batch insert data
        summary_data = []
        task_data = []
        weather_phase_data = []
        weight_data = []
        consensus_data = []
        
        for perf in performances:
            # Summary data
            summary_data.append({
                'miner_uid': perf.miner_uid,
                'period_start': perf.period_start,
                'period_end': perf.period_end,
                'period_type': perf.period_type,
                'miner_hotkey': perf.miner_hotkey,
                'total_tasks_attempted': perf.total_attempted,
                'total_tasks_completed': perf.total_completed,
                'overall_success_rate': perf.overall_success_rate,
                'overall_avg_score': perf.overall_avg_score,
                'overall_rank': perf.overall_rank,
                'performance_trend': perf.performance_trend,
                'trend_confidence': perf.trend_confidence,
                'last_active_time': perf.last_active_time,
                'consecutive_failures': perf.consecutive_failures,
                'uptime_percentage': perf.uptime_percentage,
            })
            
            # Task metrics data
            for tm in (perf.task_metrics or []):
                task_data.append({
                    'miner_uid': perf.miner_uid,
                    'period_start': perf.period_start,
                    'period_end': perf.period_end,
                    'period_type': perf.period_type,
                    'task_name': tm.task_name,
                    'tasks_attempted': tm.attempted,
                    'tasks_completed': tm.completed,
                    'tasks_scored': tm.scored,
                    'avg_score': tm.avg_score,
                    'success_rate': tm.success_rate,
                    'rank': tm.rank,
                    'best_score': tm.best_score,
                    'latest_score': tm.latest_score,
                    'percentile_rank': tm.percentile_rank,
                    'weight_contribution': tm.weight_contribution,
                    'task_specific_metrics': json.dumps(tm.task_specific_metrics) if tm.task_specific_metrics else None,
                })
                
            # Weather phase data
            for wpm in (perf.weather_phases or []):
                weather_phase_data.append({
                    'miner_uid': perf.miner_uid,
                    'period_start': perf.period_start,
                    'period_end': perf.period_end,
                    'period_type': perf.period_type,
                    'phase_type': wpm.phase_type,
                    'runs_with_phase_scores': wpm.runs_with_phase_scores,
                    'avg_phase_score': wpm.avg_phase_score,
                    'best_phase_score': wpm.best_phase_score,
                    'latest_phase_score': wpm.latest_phase_score,
                    'phase_rank': wpm.phase_rank,
                    'avg_scoring_lag_days': wpm.avg_scoring_lag_days,
                    'data_availability_summary': json.dumps(wpm.data_availability_summary) if wpm.data_availability_summary else None,
                })
                
            # Weight calculation data
            if any([perf.submitted_weight, perf.raw_calculated_weight, perf.scoring_pathway]):
                weight_data.append({
                    'miner_uid': perf.miner_uid,
                    'period_start': perf.period_start,
                    'period_end': perf.period_end,
                    'period_type': perf.period_type,
                    'submitted_weight': perf.submitted_weight,
                    'raw_calculated_weight': perf.raw_calculated_weight,
                    'excellence_weight': perf.excellence_weight,
                    'diversity_weight': perf.diversity_weight,
                    'scoring_pathway': perf.scoring_pathway,
                    'pathway_details': json.dumps(perf.pathway_details) if perf.pathway_details else None,
                    'multi_task_bonus': perf.multi_task_bonus,
                })
                
            # Consensus data
            if any([perf.incentive, perf.consensus_rank, perf.weight_submission_block]):
                consensus_data.append({
                    'miner_uid': perf.miner_uid,
                    'period_start': perf.period_start,
                    'period_end': perf.period_end,
                    'period_type': perf.period_type,
                    'incentive': perf.incentive,
                    'consensus_rank': perf.consensus_rank,
                    'weight_submission_block': perf.weight_submission_block,
                    'consensus_block': perf.consensus_block,
                })
        
        # Execute batch inserts with upserts
        if summary_data:
            await self._batch_upsert(miner_performance_summary_table, summary_data, 
                                   ['miner_uid', 'period_start', 'period_end', 'period_type'])
            
        if task_data:
            await self._batch_upsert(task_performance_metrics_table, task_data,
                                   ['miner_uid', 'period_start', 'period_end', 'period_type', 'task_name'])
            
        if weather_phase_data:
            await self._batch_upsert(weather_performance_phases_table, weather_phase_data,
                                   ['miner_uid', 'period_start', 'period_end', 'period_type', 'phase_type'])
            
        if weight_data:
            await self._batch_upsert(miner_weight_calculations_table, weight_data,
                                   ['miner_uid', 'period_start', 'period_end', 'period_type'])
            
        if consensus_data:
            await self._batch_upsert(miner_consensus_data_table, consensus_data,
                                   ['miner_uid', 'period_start', 'period_end', 'period_type'])
            
        logger.info("Successfully stored all normalized performance data")

    async def _batch_upsert(self, table, data_list: List[Dict], conflict_columns: List[str]):
        """Perform batch upsert operation."""
        if not data_list:
            return
            
        # Use PostgreSQL ON CONFLICT DO UPDATE
        stmt = postgresql.insert(table).values(data_list)
        
        # Build update dictionary for all columns except the conflict columns
        update_dict = {}
        for col in table.columns:
            if col.name not in conflict_columns:
                update_dict[col.name] = stmt.excluded[col.name]
                
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_=update_dict
        )
        
        await self.db_manager.execute(stmt)

    async def _get_active_miners(self, start: datetime, end: datetime) -> List[str]:
        """Get list of miners active during the period."""
        # Reuse logic from original calculator
        registered_miners_query = sa.select(node_table.c.uid).where(
            sa.and_(
                node_table.c.hotkey.isnot(None),
                node_table.c.uid >= 0,
                node_table.c.uid < 256
            )
        )
        
        result = await self.db_manager.execute(registered_miners_query)
        registered_uids = {str(row[0]) for row in result.fetchall()}
        
        if not registered_uids:
            return []
            
        # Get active miners from all task types
        weather_miners = sa.select(weather_miner_responses_table.c.miner_uid.distinct()).select_from(
            weather_miner_responses_table.join(weather_forecast_runs_table)
        ).where(
            weather_forecast_runs_table.c.run_initiation_time.between(start, end)
        )
        
        soil_miners = sa.select(soil_moisture_history_table.c.miner_uid.distinct()).where(
            soil_moisture_history_table.c.scored_at.between(start, end)
        )
        
        geo_miners = sa.select(geomagnetic_history_table.c.miner_uid.distinct()).where(
            geomagnetic_history_table.c.query_time.between(start, end)
        )
        
        all_active_miners = set()
        for query in [weather_miners, soil_miners, geo_miners]:
            result = await self.db_manager.execute(query)
            all_active_miners.update([row[0] for row in result.fetchall()])
            
        return [uid for uid in all_active_miners if uid in registered_uids]

    async def _get_miner_hotkey(self, miner_uid: str) -> Optional[str]:
        """Get miner hotkey from node table."""
        query = sa.select(node_table.c.hotkey).where(node_table.c.uid == int(miner_uid))
        result = await self.db_manager.execute(query)
        row = result.fetchone()
        return row[0] if row else None 