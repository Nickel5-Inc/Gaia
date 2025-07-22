import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import json

import numpy as np
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from gaia.database.validator_schema import (
    miner_performance_stats_table,
    weather_miner_scores_table,
    weather_miner_responses_table, 
    weather_forecast_runs_table,
    soil_moisture_history_table,
    geomagnetic_history_table,
    node_table
)
from gaia.tasks.base.deterministic_job_id import DeterministicJobID

logger = logging.getLogger(__name__)

@dataclass
class TaskMetrics:
    """Container for task-specific performance metrics."""
    attempted: int = 0
    completed: int = 0
    scored: int = 0
    avg_score: Optional[float] = None
    success_rate: Optional[float] = None
    best_score: Optional[float] = None
    latest_score: Optional[float] = None
    rank: Optional[int] = None
    additional_metrics: Dict[str, Any] = None

@dataclass
class MinerPerformance:
    """Complete performance profile for a miner."""
    miner_uid: str
    miner_hotkey: str
    period_start: datetime
    period_end: datetime
    period_type: str
    
    # Overall metrics
    total_attempted: int = 0
    total_completed: int = 0
    total_scored: int = 0
    overall_success_rate: Optional[float] = None
    overall_avg_score: Optional[float] = None
    overall_rank: Optional[int] = None
    
    # Task-specific metrics
    weather: TaskMetrics = None
    soil_moisture: TaskMetrics = None
    geomagnetic: TaskMetrics = None
    
    # === NEW WEIGHT CALCULATION PIPELINE FIELDS ===
    submitted_weight: Optional[float] = None
    raw_calculated_weight: Optional[float] = None
    excellence_weight: Optional[float] = None
    diversity_weight: Optional[float] = None
    scoring_pathway: Optional[str] = None  # 'excellence', 'diversity', 'none'
    pathway_details: Optional[Dict[str, Any]] = None
    
    # === NEW CHAIN CONSENSUS INTEGRATION FIELDS ===
    incentive: Optional[float] = None
    consensus_rank: Optional[int] = None
    weight_submission_block: Optional[int] = None
    consensus_block: Optional[int] = None
    
    # === NEW TASK WEIGHT CONTRIBUTIONS FIELDS ===
    weather_weight_contribution: Optional[float] = None
    geomagnetic_weight_contribution: Optional[float] = None
    soil_weight_contribution: Optional[float] = None
    multi_task_bonus: Optional[float] = None
    
    # === NEW PERFORMANCE ANALYSIS FIELDS ===
    percentile_rank_weather: Optional[float] = None
    percentile_rank_geomagnetic: Optional[float] = None
    percentile_rank_soil: Optional[float] = None
    excellence_qualified_tasks: Optional[List[str]] = None
    validator_hotkey: Optional[str] = None
    
    # Trend and status
    performance_trend: Optional[str] = None
    trend_confidence: Optional[float] = None
    last_active_time: Optional[datetime] = None
    consecutive_failures: int = 0
    uptime_percentage: Optional[float] = None
    
    detailed_metrics: Dict[str, Any] = None
    score_distribution: Dict[str, Any] = None

class MinerPerformanceCalculator:
    """Calculates and manages comprehensive miner performance statistics."""
    
    def __init__(self, db_manager):
        """
        Initialize with database manager instead of raw connection to handle connection lifecycle.
        
        Args:
            db_manager: Database manager instance that handles connection lifecycle
        """
        self.db_manager = db_manager
        self.validator_hotkey: Optional[str] = None  # Will be set by validator
    
    def set_validator_context(self, validator_hotkey: str):
        """Set the validator hotkey for tracking which validator calculated the stats."""
        self.validator_hotkey = validator_hotkey
    
    def _integrate_pathway_data(self, performance: MinerPerformance, miner_uid: str):
        """Integrate pathway tracking data from weight calculations into performance stats."""
        try:
            # Check if we have pathway data for this miner
            if (hasattr(self, '_current_pathway_data') and 
                self._current_pathway_data and 
                int(miner_uid) in self._current_pathway_data):
                
                pathway_data = self._current_pathway_data[int(miner_uid)]
                
                # Integrate weight calculation pipeline data
                performance.submitted_weight = pathway_data.get('submitted_weight')
                performance.raw_calculated_weight = pathway_data.get('raw_calculated_weight')
                performance.excellence_weight = pathway_data.get('excellence_weight')
                performance.diversity_weight = pathway_data.get('diversity_weight')
                performance.scoring_pathway = pathway_data.get('scoring_pathway')
                performance.pathway_details = pathway_data.get('pathway_details')
                
                # Integrate task weight contributions
                performance.weather_weight_contribution = pathway_data.get('weather_weight_contribution')
                performance.geomagnetic_weight_contribution = pathway_data.get('geomagnetic_weight_contribution')
                performance.soil_weight_contribution = pathway_data.get('soil_weight_contribution')
                performance.multi_task_bonus = pathway_data.get('multi_task_bonus')
                
                # Extract percentile ranks from pathway details if available
                if pathway_data.get('pathway_details') and 'percentile_ranks' in pathway_data['pathway_details']:
                    percentile_ranks = pathway_data['pathway_details']['percentile_ranks']
                    performance.percentile_rank_weather = percentile_ranks.get('weather')
                    performance.percentile_rank_geomagnetic = percentile_ranks.get('geomagnetic')
                    performance.percentile_rank_soil = percentile_ranks.get('soil')
                
                # Extract excellence qualified tasks
                if pathway_data.get('pathway_details') and 'excellence_qualified_tasks' in pathway_data['pathway_details']:
                    performance.excellence_qualified_tasks = pathway_data['pathway_details']['excellence_qualified_tasks']
                
                logger.debug(f"Integrated pathway data for miner {miner_uid}: pathway={performance.scoring_pathway}, weight={performance.submitted_weight}")
            
            # === NEW: Integrate consensus data if available ===
            if (hasattr(self, '_current_consensus_data') and 
                self._current_consensus_data and 
                int(miner_uid) in self._current_consensus_data):
                
                consensus_data = self._current_consensus_data[int(miner_uid)]
                
                # Integrate chain consensus integration data
                performance.incentive = consensus_data.get('incentive')
                performance.consensus_rank = consensus_data.get('consensus_rank')
                performance.consensus_block = consensus_data.get('consensus_block')
                
                # Also capture validator hotkey from consensus data if not set
                if not performance.validator_hotkey and consensus_data.get('validator_hotkey'):
                    performance.validator_hotkey = consensus_data.get('validator_hotkey')
                
                logger.debug(f"Integrated consensus data for miner {miner_uid}: incentive={performance.incentive}, rank={performance.consensus_rank}")
                
        except Exception as e:
            logger.warning(f"Error integrating pathway data for miner {miner_uid}: {e}")
            # Continue without pathway data - the rest of the performance stats are still valid
        
    async def calculate_period_stats(
        self,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
        miner_uids: Optional[List[str]] = None
    ) -> List[MinerPerformance]:
        """
        Calculate comprehensive performance statistics for all miners in a period.
        
        Args:
            period_type: Type of period (daily, weekly, monthly, all_time)
            period_start: Start of calculation period
            period_end: End of calculation period  
            miner_uids: Optional list of specific miners to calculate (None = all)
            
        Returns:
            List of MinerPerformance objects
        """
        logger.info(f"Calculating {period_type} performance stats for period {period_start} to {period_end}")
        
        # Get all active miners if not specified
        if miner_uids is None:
            miner_uids = await self._get_active_miners(period_start, period_end)
        
        performances = []
        
        for miner_uid in miner_uids:
            try:
                # Get miner hotkey
                miner_hotkey = await self._get_miner_hotkey(miner_uid)
                if not miner_hotkey:
                    logger.warning(f"No hotkey found for miner UID {miner_uid}")
                    continue
                
                performance = MinerPerformance(
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    period_start=period_start,
                    period_end=period_end,
                    period_type=period_type,
                    validator_hotkey=self.validator_hotkey  # Automatically set from calculator context
                )
                
                # Calculate task-specific metrics
                performance.weather = await self._calculate_weather_metrics(
                    miner_uid, miner_hotkey, period_start, period_end
                )
                performance.soil_moisture = await self._calculate_soil_moisture_metrics(
                    miner_uid, miner_hotkey, period_start, period_end
                )
                performance.geomagnetic = await self._calculate_geomagnetic_metrics(
                    miner_uid, miner_hotkey, period_start, period_end
                )
                
                # Calculate overall metrics
                self._calculate_overall_metrics(performance)
                
                # === NEW: Integrate pathway tracking data if available ===
                self._integrate_pathway_data(performance, miner_uid)
                
                # Calculate trends and status
                await self._calculate_trends_and_status(performance)
                
                # Calculate detailed metrics and distributions
                self._calculate_detailed_metrics(performance)
                
                performances.append(performance)
                
            except Exception as e:
                logger.error(f"Error calculating performance for miner {miner_uid}: {e}")
                continue
        
        # Calculate ranks across all miners
        self._calculate_ranks(performances)
        
        return performances
    
    async def _get_active_miners(self, start: datetime, end: datetime) -> List[str]:
        """Get list of miners active during the period and currently registered."""
        # Get currently registered miners from node table
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
            logger.warning("No registered miners found in node table")
            return []
        
        # Union query to find miners active during the period
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
        
        # Combine all active miners and filter by registered status
        all_active_miners = set()
        for query in [weather_miners, soil_miners, geo_miners]:
            result = await self.db_manager.execute(query)
            all_active_miners.update([row[0] for row in result.fetchall()])
        
        # Only return miners that are both active AND currently registered
        active_registered = [uid for uid in all_active_miners if uid in registered_uids]
        
        logger.info(f"Found {len(active_registered)} active registered miners out of {len(all_active_miners)} total active miners")
        return active_registered
    
    async def _get_miner_hotkey(self, miner_uid: str) -> Optional[str]:
        """Get miner hotkey from node table."""
        query = sa.select(node_table.c.hotkey).where(node_table.c.uid == int(miner_uid))
        result = await self.db_manager.execute(query)
        row = result.fetchone()
        return row[0] if row else None
    
    async def _calculate_weather_metrics(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> TaskMetrics:
        """
        Calculate weather forecasting performance metrics.
        
        Note: Weather scoring has two phases:
        - Day1 QC scores (20% weight): Available quickly after forecast submission
        - ERA5 final scores (80% weight): Available ~10 days later after ground truth data
        
        This method captures both by looking at forecast run initiation time, not score calculation time.
        """
        metrics = TaskMetrics(additional_metrics={})
        
        # Join tables to get comprehensive weather data
        # Note: Using LEFT JOIN on weather_miner_scores to include runs where scores
        # may not have been calculated yet (due to ~10 day delay for ERA5 final scores)
        query = sa.select(
            weather_miner_responses_table.c.status,
            weather_miner_scores_table.c.score,
            weather_miner_scores_table.c.score_type,
            weather_forecast_runs_table.c.run_initiation_time,
            weather_miner_scores_table.c.calculation_time
        ).select_from(
            weather_miner_responses_table
            .join(weather_forecast_runs_table)
            .outerjoin(weather_miner_scores_table)
        ).where(
            sa.and_(
                weather_miner_responses_table.c.miner_uid == int(miner_uid),
                weather_forecast_runs_table.c.run_initiation_time.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        
        if not rows:
            return metrics
        
        # Convert to numpy arrays for vectorized operations
        row_data = np.array(rows, dtype=object)
        if len(row_data) == 0:
            return metrics
            
        statuses = row_data[:, 0]
        scores = row_data[:, 1]
        score_types = row_data[:, 2]  
        run_times = row_data[:, 3]
        calc_times = row_data[:, 4] if row_data.shape[1] > 4 else None
        
        # Vectorized status counting
        unique_statuses, status_counts_arr = np.unique(statuses, return_counts=True)
        status_counts = dict(zip(unique_statuses, status_counts_arr))
        
        # Filter valid scores and vectorize score operations
        valid_score_mask = scores != None
        valid_scores = scores[valid_score_mask].astype(np.float64)
        valid_score_types = score_types[valid_score_mask]
        valid_run_times = run_times[valid_score_mask]
        
        # Find latest score using vectorized operations
        if len(valid_scores) > 0:
            latest_idx = np.argmax(valid_run_times)
            metrics.latest_score = float(valid_scores[latest_idx])
        
        # Vectorized score calculations by type
        score_by_type = {}
        unique_types = np.unique(valid_score_types)
        for score_type in unique_types:
            if score_type is not None:
                type_mask = valid_score_types == score_type
                type_scores = valid_scores[type_mask]
                if len(type_scores) > 0:
                    score_by_type[score_type] = np.mean(type_scores)
        
        # Calculate metrics using vectorized operations
        metrics.attempted = len(row_data)
        metrics.completed = status_counts.get('completed', 0) + status_counts.get('verified', 0)
        metrics.scored = len(valid_scores)
        
        if metrics.attempted > 0:
            metrics.success_rate = metrics.completed / metrics.attempted
        
        if len(valid_scores) > 0:
            metrics.avg_score = float(np.mean(valid_scores))
            metrics.best_score = float(np.max(valid_scores))
        
        # Analyze scoring delays for weather forecasts
        delayed_scores = 0
        if calc_times is not None and len(valid_run_times) > 0:
            # Check for scores calculated significantly after run initiation
            for i, (run_time, calc_time) in enumerate(zip(valid_run_times, calc_times[valid_score_mask])):
                if calc_time is not None and run_time is not None:
                    delay_days = (calc_time - run_time).days if calc_time > run_time else 0
                    if delay_days > 5:  # Scores calculated more than 5 days after forecast
                        delayed_scores += 1
        
        # Store detailed weather metrics
        metrics.additional_metrics = {
            'status_breakdown': status_counts,
            'score_by_type': score_by_type,
            'total_forecasts': metrics.attempted,
            'delayed_scores_count': delayed_scores,
            'delayed_scores_ratio': delayed_scores / max(len(valid_scores), 1) if delayed_scores > 0 else 0.0
        }
        
        return metrics
    
    async def _calculate_soil_moisture_metrics(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> TaskMetrics:
        """Calculate soil moisture prediction performance metrics."""
        metrics = TaskMetrics(additional_metrics={})
        
        query = sa.select(
            soil_moisture_history_table.c.surface_rmse,
            soil_moisture_history_table.c.rootzone_rmse,
            soil_moisture_history_table.c.surface_structure_score,
            soil_moisture_history_table.c.rootzone_structure_score,
            soil_moisture_history_table.c.scored_at
        ).where(
            sa.and_(
                soil_moisture_history_table.c.miner_uid == miner_uid,
                soil_moisture_history_table.c.scored_at.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        
        if not rows:
            return metrics
        
        # Convert to numpy arrays for vectorized operations
        row_data = np.array(rows, dtype=object)
        if len(row_data) == 0:
            return metrics
            
        surface_rmses = row_data[:, 0]
        rootzone_rmses = row_data[:, 1]  
        surface_scores = row_data[:, 2]
        rootzone_scores = row_data[:, 3]
        scored_at_times = row_data[:, 4]
        
        # Create masks for valid values and convert to float arrays
        surf_rmse_mask = surface_rmses != None
        root_rmse_mask = rootzone_rmses != None
        surf_score_mask = surface_scores != None
        root_score_mask = rootzone_scores != None
        
        valid_surf_rmses = surface_rmses[surf_rmse_mask].astype(np.float64)
        valid_root_rmses = rootzone_rmses[root_rmse_mask].astype(np.float64)
        valid_surf_scores = surface_scores[surf_score_mask].astype(np.float64)
        valid_root_scores = rootzone_scores[root_score_mask].astype(np.float64)
        
        # Calculate combined scores using vectorized operations
        combined_mask = surf_score_mask & root_score_mask
        if np.any(combined_mask):
            surf_combined = surface_scores[combined_mask].astype(np.float64)
            root_combined = rootzone_scores[combined_mask].astype(np.float64)
            combined_scores = (surf_combined + root_combined) / 2.0
            combined_times = scored_at_times[combined_mask]
            
            # Find latest score using vectorized operations
            latest_idx = np.argmax(combined_times)
            latest_score = float(combined_scores[latest_idx])
        else:
            combined_scores = np.array([])
            latest_score = None
        
        metrics.attempted = len(row_data)  # All scored entries are attempted
        metrics.completed = len(row_data)  # All scored entries are completed
        metrics.scored = len(combined_scores)
        metrics.success_rate = 1.0 if metrics.attempted > 0 else None
        
        if len(combined_scores) > 0:
            metrics.avg_score = float(np.mean(combined_scores))
            metrics.best_score = float(np.max(combined_scores))
            metrics.latest_score = latest_score
        
        # Store detailed soil moisture metrics using vectorized calculations
        metrics.additional_metrics = {
            'avg_surface_rmse': float(np.mean(valid_surf_rmses)) if len(valid_surf_rmses) > 0 else None,
            'avg_rootzone_rmse': float(np.mean(valid_root_rmses)) if len(valid_root_rmses) > 0 else None,
            'avg_surface_structure': float(np.mean(valid_surf_scores)) if len(valid_surf_scores) > 0 else None,
            'avg_rootzone_structure': float(np.mean(valid_root_scores)) if len(valid_root_scores) > 0 else None,
            'total_predictions': len(row_data)
        }
        
        return metrics
    
    async def _calculate_geomagnetic_metrics(
        self, miner_uid: str, miner_hotkey: str, start: datetime, end: datetime
    ) -> TaskMetrics:
        """Calculate geomagnetic prediction performance metrics."""
        metrics = TaskMetrics(additional_metrics={})
        
        query = sa.select(
            geomagnetic_history_table.c.score,
            geomagnetic_history_table.c.predicted_value,
            geomagnetic_history_table.c.ground_truth_value,
            geomagnetic_history_table.c.query_time
        ).where(
            sa.and_(
                geomagnetic_history_table.c.miner_uid == miner_uid,
                geomagnetic_history_table.c.query_time.between(start, end)
            )
        )
        
        result = await self.db_manager.execute(query)
        rows = result.fetchall()
        
        if not rows:
            return metrics
        
        # Convert to numpy arrays for vectorized operations
        row_data = np.array(rows, dtype=object)
        if len(row_data) == 0:
            return metrics
            
        scores = row_data[:, 0]
        pred_vals = row_data[:, 1]
        truth_vals = row_data[:, 2]
        query_times = row_data[:, 3]
        
        # Create masks for valid values
        score_mask = scores != None
        pred_truth_mask = (pred_vals != None) & (truth_vals != None)
        
        # Convert valid scores to float array
        valid_scores = scores[score_mask].astype(np.float64)
        valid_query_times = query_times[score_mask]
        
        # Find latest score using vectorized operations
        latest_score = None
        if len(valid_scores) > 0:
            latest_idx = np.argmax(valid_query_times)
            latest_score = float(valid_scores[latest_idx])
        
        # Calculate prediction errors using vectorized operations
        valid_errors = np.array([])
        if np.any(pred_truth_mask):
            valid_preds = pred_vals[pred_truth_mask].astype(np.float64)
            valid_truths = truth_vals[pred_truth_mask].astype(np.float64)
            valid_errors = np.abs(valid_preds - valid_truths)
        
        metrics.attempted = len(row_data)
        metrics.completed = len(row_data)  # All entries in history are completed
        metrics.scored = len(valid_scores)
        metrics.success_rate = 1.0 if metrics.attempted > 0 else None
        
        if len(valid_scores) > 0:
            metrics.avg_score = float(np.mean(valid_scores))
            metrics.best_score = float(np.max(valid_scores))
            metrics.latest_score = latest_score
        
        # Store detailed geomagnetic metrics using vectorized calculations
        accuracy_threshold = 50.0
        prediction_accuracy = None
        if len(valid_errors) > 0:
            prediction_accuracy = float(np.mean(valid_errors < accuracy_threshold))
        
        metrics.additional_metrics = {
            'avg_prediction_error': float(np.mean(valid_errors)) if len(valid_errors) > 0 else None,
            'total_predictions': len(row_data),
            'prediction_accuracy': prediction_accuracy
        }
        
        return metrics
    
    def _calculate_overall_metrics(self, performance: MinerPerformance):
        """Calculate overall aggregated metrics from task-specific metrics."""
        # Sum up totals
        performance.total_attempted = (
            (performance.weather.attempted if performance.weather else 0) +
            (performance.soil_moisture.attempted if performance.soil_moisture else 0) +
            (performance.geomagnetic.attempted if performance.geomagnetic else 0)
        )
        
        performance.total_completed = (
            (performance.weather.completed if performance.weather else 0) +
            (performance.soil_moisture.completed if performance.soil_moisture else 0) +
            (performance.geomagnetic.completed if performance.geomagnetic else 0)
        )
        
        performance.total_scored = (
            (performance.weather.scored if performance.weather else 0) +
            (performance.soil_moisture.scored if performance.soil_moisture else 0) +
            (performance.geomagnetic.scored if performance.geomagnetic else 0)
        )
        
        # Calculate overall success rate
        if performance.total_attempted > 0:
            performance.overall_success_rate = performance.total_completed / performance.total_attempted
        
        # Calculate weighted average score using numpy
        task_scores = []
        task_weights = []
        
        for task_metrics in [performance.weather, performance.soil_moisture, performance.geomagnetic]:
            if task_metrics and task_metrics.avg_score is not None and task_metrics.scored > 0:
                task_scores.append(task_metrics.avg_score)
                task_weights.append(task_metrics.scored)
        
        if task_scores:
            scores_array = np.array(task_scores, dtype=np.float64)
            weights_array = np.array(task_weights, dtype=np.float64)
            performance.overall_avg_score = float(np.average(scores_array, weights=weights_array))
    
    async def _calculate_trends_and_status(self, performance: MinerPerformance):
        """Calculate performance trends and miner status indicators using numpy."""
        # Get recent performance data for trend analysis
        trend_period_days = 14  # Look back 2 weeks for trend
        trend_start = performance.period_end - timedelta(days=trend_period_days)
        
        # Get historical performance data using vectorized operations
        trend_data = await self._get_trend_data(
            performance.miner_uid, trend_start, performance.period_end
        )
        
        if trend_data and len(trend_data) >= 3:  # Need at least 3 points for trend
            scores = np.array([d['score'] for d in trend_data], dtype=np.float64)
            times = np.array([d['time'] for d in trend_data])
            
            # Convert times to numeric (days since start) for regression
            time_numeric = np.array([(t - trend_start).days for t in times], dtype=np.float64)
            
            # Calculate linear trend using numpy
            if len(scores) > 1:
                # Linear regression: slope indicates trend direction
                coeffs = np.polyfit(time_numeric, scores, 1)
                slope = coeffs[0]
                
                # Calculate correlation coefficient for trend confidence
                correlation = np.corrcoef(time_numeric, scores)[0, 1]
                trend_confidence = abs(correlation) if not np.isnan(correlation) else 0.0
                
                # Classify trend based on slope and significance
                slope_threshold = 0.01  # Minimum slope to consider significant
                if abs(slope) < slope_threshold or trend_confidence < 0.3:
                    trend = "stable"
                elif slope > 0:
                    trend = "improving"
                else:
                    trend = "declining"
                
                performance.performance_trend = trend
                performance.trend_confidence = float(trend_confidence)
            else:
                performance.performance_trend = "insufficient_data"
                performance.trend_confidence = 0.0
        else:
            performance.performance_trend = "insufficient_data"
            performance.trend_confidence = 0.0
        
        # Calculate uptime and activity metrics
        performance.last_active_time = performance.period_end  # Will be updated with real data
        performance.uptime_percentage = performance.overall_success_rate
        
        # Calculate consecutive failures (simplified - would need more complex query)
        performance.consecutive_failures = 0  # Placeholder
    
    async def _get_trend_data(self, miner_uid: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get historical performance data for trend analysis."""
        # This would query recent performance data across all tasks
        # For now, return empty list - would need to implement based on specific requirements
        return []
    
    def _calculate_detailed_metrics(self, performance: MinerPerformance):
        """Calculate detailed metrics and score distributions."""
        # Aggregate all detailed metrics
        detailed = {}
        
        if performance.weather and performance.weather.additional_metrics:
            detailed['weather'] = performance.weather.additional_metrics
        
        if performance.soil_moisture and performance.soil_moisture.additional_metrics:
            detailed['soil_moisture'] = performance.soil_moisture.additional_metrics
        
        if performance.geomagnetic and performance.geomagnetic.additional_metrics:
            detailed['geomagnetic'] = performance.geomagnetic.additional_metrics
        
        performance.detailed_metrics = detailed
        
        # Calculate score distribution
        all_scores = []
        if performance.weather and performance.weather.avg_score:
            all_scores.append(('weather', performance.weather.avg_score))
        if performance.soil_moisture and performance.soil_moisture.avg_score:
            all_scores.append(('soil_moisture', performance.soil_moisture.avg_score))
        if performance.geomagnetic and performance.geomagnetic.avg_score:
            all_scores.append(('geomagnetic', performance.geomagnetic.avg_score))
        
        if all_scores:
            scores_only = np.array([score for _, score in all_scores], dtype=np.float64)
            performance.score_distribution = {
                'by_task': dict(all_scores),
                'overall_min': float(np.min(scores_only)),
                'overall_max': float(np.max(scores_only)),
                'overall_std': float(np.std(scores_only)) if len(scores_only) > 1 else 0.0,
                'overall_median': float(np.median(scores_only)),
                'percentile_25': float(np.percentile(scores_only, 25)),
                'percentile_75': float(np.percentile(scores_only, 75))
            }
    
    def _calculate_ranks(self, performances: List[MinerPerformance]):
        """Calculate rankings across all miners for each metric using numpy."""
        if not performances:
            return
        
        # Overall ranking by average score using numpy
        scored_performances = [p for p in performances if p.overall_avg_score is not None]
        if scored_performances:
            scores = np.array([p.overall_avg_score for p in scored_performances], dtype=np.float64)
            # Use argsort to get ranking indices (descending order)
            rank_indices = np.argsort(-scores)  # Negative for descending order
            ranks = np.empty_like(rank_indices)
            ranks[rank_indices] = np.arange(1, len(rank_indices) + 1)
            
            for i, performance in enumerate(scored_performances):
                performance.overall_rank = int(ranks[i])
        
        # Task-specific rankings using numpy
        for task_name in ['weather', 'soil_moisture', 'geomagnetic']:
            task_performances = [
                p for p in performances 
                if getattr(p, task_name) and getattr(p, task_name).avg_score is not None
            ]
            
            if task_performances:
                task_scores = np.array([
                    getattr(p, task_name).avg_score for p in task_performances
                ], dtype=np.float64)
                
                # Calculate ranks using numpy
                rank_indices = np.argsort(-task_scores)  # Descending order
                ranks = np.empty_like(rank_indices)
                ranks[rank_indices] = np.arange(1, len(rank_indices) + 1)
                
                for i, performance in enumerate(task_performances):
                    getattr(performance, task_name).rank = int(ranks[i])
    
    async def save_performance_stats(self, performances: List[MinerPerformance]):
        """Save calculated performance statistics to the database."""
        if not performances:
            return
        
        logger.info(f"Saving performance stats for {len(performances)} miners")
        
        # Prepare batch insert data
        insert_data = []
        
        for perf in performances:
            row_data = {
                'miner_uid': perf.miner_uid,
                'miner_hotkey': perf.miner_hotkey,
                'period_start': perf.period_start,
                'period_end': perf.period_end,
                'period_type': perf.period_type,
                
                # Overall metrics
                'total_tasks_attempted': perf.total_attempted,
                'total_tasks_completed': perf.total_completed,
                'total_tasks_scored': perf.total_scored,
                'overall_success_rate': perf.overall_success_rate,
                'overall_avg_score': perf.overall_avg_score,
                'overall_rank': perf.overall_rank,
                
                # Task-specific metrics
                'weather_tasks_attempted': perf.weather.attempted if perf.weather else 0,
                'weather_tasks_completed': perf.weather.completed if perf.weather else 0,
                'weather_tasks_scored': perf.weather.scored if perf.weather else 0,
                'weather_avg_score': perf.weather.avg_score if perf.weather else None,
                'weather_success_rate': perf.weather.success_rate if perf.weather else None,
                'weather_rank': perf.weather.rank if perf.weather else None,
                'weather_best_score': perf.weather.best_score if perf.weather else None,
                'weather_latest_score': perf.weather.latest_score if perf.weather else None,
                
                'soil_moisture_tasks_attempted': perf.soil_moisture.attempted if perf.soil_moisture else 0,
                'soil_moisture_tasks_completed': perf.soil_moisture.completed if perf.soil_moisture else 0,
                'soil_moisture_tasks_scored': perf.soil_moisture.scored if perf.soil_moisture else 0,
                'soil_moisture_avg_score': perf.soil_moisture.avg_score if perf.soil_moisture else None,
                'soil_moisture_success_rate': perf.soil_moisture.success_rate if perf.soil_moisture else None,
                'soil_moisture_rank': perf.soil_moisture.rank if perf.soil_moisture else None,
                'soil_moisture_surface_rmse_avg': (
                    perf.soil_moisture.additional_metrics.get('avg_surface_rmse') 
                    if perf.soil_moisture and perf.soil_moisture.additional_metrics else None
                ),
                'soil_moisture_rootzone_rmse_avg': (
                    perf.soil_moisture.additional_metrics.get('avg_rootzone_rmse')
                    if perf.soil_moisture and perf.soil_moisture.additional_metrics else None
                ),
                'soil_moisture_best_score': perf.soil_moisture.best_score if perf.soil_moisture else None,
                'soil_moisture_latest_score': perf.soil_moisture.latest_score if perf.soil_moisture else None,
                
                'geomagnetic_tasks_attempted': perf.geomagnetic.attempted if perf.geomagnetic else 0,
                'geomagnetic_tasks_completed': perf.geomagnetic.completed if perf.geomagnetic else 0,
                'geomagnetic_tasks_scored': perf.geomagnetic.scored if perf.geomagnetic else 0,
                'geomagnetic_avg_score': perf.geomagnetic.avg_score if perf.geomagnetic else None,
                'geomagnetic_success_rate': perf.geomagnetic.success_rate if perf.geomagnetic else None,
                'geomagnetic_rank': perf.geomagnetic.rank if perf.geomagnetic else None,
                'geomagnetic_best_score': perf.geomagnetic.best_score if perf.geomagnetic else None,
                'geomagnetic_latest_score': perf.geomagnetic.latest_score if perf.geomagnetic else None,
                'geomagnetic_avg_error': (
                    perf.geomagnetic.additional_metrics.get('avg_prediction_error')
                    if perf.geomagnetic and perf.geomagnetic.additional_metrics else None
                ),
                
                # === NEW WEIGHT CALCULATION PIPELINE FIELDS ===
                'submitted_weight': perf.submitted_weight,
                'raw_calculated_weight': perf.raw_calculated_weight,
                'excellence_weight': perf.excellence_weight,
                'diversity_weight': perf.diversity_weight,
                'scoring_pathway': perf.scoring_pathway,
                'pathway_details': json.dumps(perf.pathway_details) if perf.pathway_details else None,
                
                # === NEW CHAIN CONSENSUS INTEGRATION FIELDS ===
                'incentive': perf.incentive,
                'consensus_rank': perf.consensus_rank,
                'weight_submission_block': perf.weight_submission_block,
                'consensus_block': perf.consensus_block,
                
                # === NEW TASK WEIGHT CONTRIBUTIONS FIELDS ===
                'weather_weight_contribution': perf.weather_weight_contribution,
                'geomagnetic_weight_contribution': perf.geomagnetic_weight_contribution,
                'soil_weight_contribution': perf.soil_weight_contribution,
                'multi_task_bonus': perf.multi_task_bonus,
                
                # === NEW PERFORMANCE ANALYSIS FIELDS ===
                'percentile_rank_weather': perf.percentile_rank_weather,
                'percentile_rank_geomagnetic': perf.percentile_rank_geomagnetic,
                'percentile_rank_soil': perf.percentile_rank_soil,
                'excellence_qualified_tasks': perf.excellence_qualified_tasks,
                'validator_hotkey': perf.validator_hotkey,
                
                # Trend and status
                'performance_trend': perf.performance_trend,
                'trend_confidence': perf.trend_confidence,
                'last_active_time': perf.last_active_time,
                'consecutive_failures': perf.consecutive_failures,
                'uptime_percentage': perf.uptime_percentage,
                
                'detailed_metrics': json.dumps(perf.detailed_metrics) if perf.detailed_metrics else None,
                'score_distribution': json.dumps(perf.score_distribution) if perf.score_distribution else None,
                # updated_at will be set automatically by the database server default
            }
            
            insert_data.append(row_data)
        
        # Execute upsert without creating new transaction (caller manages transactions)
        stmt = postgresql.insert(miner_performance_stats_table).values(insert_data)
        upsert_stmt = stmt.on_conflict_do_update(
            constraint='uq_mps_miner_period',
            set_={
                col.name: stmt.excluded[col.name] 
                for col in miner_performance_stats_table.columns 
                if col.name not in ['id', 'calculated_at']
            }
        )
        
        await self.db_manager.execute(upsert_stmt)
        
        logger.info(f"Successfully saved performance stats for {len(performances)} miners")
    
    async def cleanup_deregistered_miners(self) -> int:
        """
        Remove performance statistics for miners that are no longer registered.
        
        Returns:
            Number of records cleaned up
        """
        try:
            # Execute queries without creating new transaction (caller manages transactions)
            # Get currently registered miners
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
                logger.warning("No registered miners found - skipping performance stats cleanup")
                return 0
            
            # Get all miners with performance stats
            stats_miners_query = sa.select(miner_performance_stats_table.c.miner_uid.distinct())
            result = await self.db_manager.execute(stats_miners_query)
            stats_uids = {row[0] for row in result.fetchall()}
            
            # Find miners in stats but not registered
            deregistered_uids = stats_uids - registered_uids
            
            if not deregistered_uids:
                logger.debug("No deregistered miners found in performance stats")
                return 0
            
            # Delete performance stats for deregistered miners
            delete_query = sa.delete(miner_performance_stats_table).where(
                miner_performance_stats_table.c.miner_uid.in_(deregistered_uids)
            )
            
            result = await self.db_manager.execute(delete_query)
            deleted_count = result.rowcount
            
            logger.info(f"Cleaned up performance stats for {deleted_count} deregistered miners: {list(deregistered_uids)[:10]}...")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up deregistered miners: {e}")
            return 0
    
    async def cleanup_specific_miner(self, miner_uid: str, miner_hotkey: str = None) -> bool:
        """
        Remove performance statistics for a specific miner.
        
        Args:
            miner_uid: The miner's UID to clean up
            miner_hotkey: Optional hotkey for additional cleanup
            
        Returns:
            True if cleanup was successful, False otherwise
        """
        try:
            # Execute queries without creating new transaction (caller manages transactions)
            # Build delete conditions
            conditions = [miner_performance_stats_table.c.miner_uid == miner_uid]
            
            if miner_hotkey:
                conditions.append(miner_performance_stats_table.c.miner_hotkey == miner_hotkey)
            
            # Delete performance stats for the specific miner
            delete_query = sa.delete(miner_performance_stats_table).where(
                sa.or_(*conditions)
            )
            
            result = await self.db_manager.execute(delete_query)
            deleted_count = result.rowcount
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} performance records for miner UID {miner_uid}")
        
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up miner {miner_uid}: {e}")
            return False
    
    async def calculate_bulk_rankings(self, performances: List[MinerPerformance]) -> Dict[str, np.ndarray]:
        """
        Calculate bulk rankings for all miners using efficient numpy operations.
        
        Returns ranking arrays that can be used for batch updates.
        """
        if not performances:
            return {}
        
        # Extract all scores into numpy arrays for efficient ranking
        miner_uids = [p.miner_uid for p in performances]
        overall_scores = np.array([
            p.overall_avg_score if p.overall_avg_score is not None else 0.0 
            for p in performances
        ], dtype=np.float64)
        
        # Extract task-specific scores
        weather_scores = np.array([
            p.weather.avg_score if p.weather and p.weather.avg_score is not None else 0.0
            for p in performances
        ], dtype=np.float64)
        
        soil_scores = np.array([
            p.soil_moisture.avg_score if p.soil_moisture and p.soil_moisture.avg_score is not None else 0.0
            for p in performances  
        ], dtype=np.float64)
        
        geo_scores = np.array([
            p.geomagnetic.avg_score if p.geomagnetic and p.geomagnetic.avg_score is not None else 0.0
            for p in performances
        ], dtype=np.float64)
        
        # Calculate percentile rankings efficiently
        rankings = {}
        
        # Overall rankings
        overall_percentiles = np.percentile(overall_scores, 
                                          np.arange(0, 101, 10))  # Decile rankings
        rankings['overall_percentiles'] = overall_percentiles
        rankings['overall_ranks'] = np.argsort(np.argsort(-overall_scores)) + 1
        
        # Task-specific rankings
        rankings['weather_ranks'] = np.argsort(np.argsort(-weather_scores)) + 1
        rankings['soil_ranks'] = np.argsort(np.argsort(-soil_scores)) + 1
        rankings['geo_ranks'] = np.argsort(np.argsort(-geo_scores)) + 1
        
        # Calculate performance tiers using numpy quantiles
        overall_quantiles = np.quantile(overall_scores[overall_scores > 0], [0.25, 0.5, 0.75, 0.9])
        rankings['performance_tiers'] = np.digitize(overall_scores, overall_quantiles)
        
        return rankings
    
    def batch_calculate_score_distributions(self, all_scores: List[np.ndarray]) -> Dict[str, float]:
        """
        Calculate score distributions across all miners using vectorized operations.
        """
        if not all_scores:
            return {}
        
        # Concatenate all scores for global statistics
        combined_scores = np.concatenate([scores for scores in all_scores if len(scores) > 0])
        
        if len(combined_scores) == 0:
            return {}
        
        # Calculate comprehensive distribution statistics
        distribution = {
            'global_mean': float(np.mean(combined_scores)),
            'global_std': float(np.std(combined_scores)),
            'global_median': float(np.median(combined_scores)),
            'global_min': float(np.min(combined_scores)),
            'global_max': float(np.max(combined_scores)),
            'global_range': float(np.ptp(combined_scores)),  # Peak-to-peak (max - min)
            'percentile_10': float(np.percentile(combined_scores, 10)),
            'percentile_25': float(np.percentile(combined_scores, 25)),
            'percentile_75': float(np.percentile(combined_scores, 75)),
            'percentile_90': float(np.percentile(combined_scores, 90)),
            'iqr': float(np.percentile(combined_scores, 75) - np.percentile(combined_scores, 25)),
            'skewness': float(self._calculate_skewness(combined_scores)),
            'kurtosis': float(self._calculate_kurtosis(combined_scores))
        }
        
        return distribution
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """Calculate skewness using numpy operations."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        return np.mean(((data - mean) / std) ** 3)
    
    def _calculate_kurtosis(self, data: np.ndarray) -> float:
        """Calculate kurtosis using numpy operations."""
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
        return np.mean(((data - mean) / std) ** 4) - 3  # Excess kurtosis

async def calculate_daily_stats(db_manager, target_date: datetime):
    """Calculate daily performance statistics for all miners."""
    calculator = MinerPerformanceCalculator(db_manager)
    
    period_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = period_start + timedelta(days=1)
    
    performances = await calculator.calculate_period_stats(
        period_type='daily',
        period_start=period_start,
        period_end=period_end
    )
    
    await calculator.save_performance_stats(performances)
    return performances

async def calculate_weekly_stats(db_manager, week_start: datetime):
    """Calculate weekly performance statistics for all miners."""
    calculator = MinerPerformanceCalculator(db_manager)
    
    period_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    period_end = period_start + timedelta(days=7)
    
    performances = await calculator.calculate_period_stats(
        period_type='weekly', 
        period_start=period_start,
        period_end=period_end
    )
    
    await calculator.save_performance_stats(performances)
    return performances

async def calculate_monthly_stats(db_manager, month_start: datetime):
    """Calculate monthly performance statistics for all miners."""
    calculator = MinerPerformanceCalculator(db_manager)
    
    # Calculate end of month
    if month_start.month == 12:
        period_end = month_start.replace(year=month_start.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        period_end = month_start.replace(month=month_start.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    
    period_start = month_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    performances = await calculator.calculate_period_stats(
        period_type='monthly',
        period_start=period_start,
        period_end=period_end
    )
    
    await calculator.save_performance_stats(performances)
    return performances 