"""
Example usage of the optimized miner performance statistics system.

This demonstrates how to calculate and retrieve comprehensive miner performance 
statistics across all task types using vectorized numpy operations.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import numpy as np
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

from gaia.tasks.base.miner_performance_calculator import (
    MinerPerformanceCalculator,
    calculate_daily_stats,
    calculate_weekly_stats,
    calculate_monthly_stats
)
from gaia.database.validator_schema import miner_performance_stats_table

async def example_calculate_performance_stats():
    """Example: Calculate performance statistics for all miners."""
    
    # Create database connection (replace with your actual connection)
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/gaia")
    
    async with engine.begin() as conn:
        # Calculate daily stats for yesterday
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        daily_performances = await calculate_daily_stats(conn, yesterday)
        
        print(f"Calculated daily stats for {len(daily_performances)} miners")
        
        # Calculate weekly stats for last week
        week_start = yesterday - timedelta(days=7)
        weekly_performances = await calculate_weekly_stats(conn, week_start)
        
        print(f"Calculated weekly stats for {len(weekly_performances)} miners")

async def example_query_performance_stats():
    """Example: Query calculated performance statistics."""
    
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/gaia")
    
    async with engine.begin() as conn:
        # Get top performers in each category for the last week
        query = sa.select(
            miner_performance_stats_table.c.miner_hotkey,
            miner_performance_stats_table.c.overall_avg_score,
            miner_performance_stats_table.c.overall_rank,
            miner_performance_stats_table.c.weather_avg_score,
            miner_performance_stats_table.c.weather_rank,
            miner_performance_stats_table.c.soil_moisture_avg_score,
            miner_performance_stats_table.c.soil_moisture_rank,
            miner_performance_stats_table.c.geomagnetic_avg_score,
            miner_performance_stats_table.c.geomagnetic_rank,
            miner_performance_stats_table.c.total_tasks_attempted,
            miner_performance_stats_table.c.overall_success_rate,
            miner_performance_stats_table.c.performance_trend
        ).where(
            sa.and_(
                miner_performance_stats_table.c.period_type == 'weekly',
                miner_performance_stats_table.c.overall_rank.isnot(None)
            )
        ).order_by(
            miner_performance_stats_table.c.overall_rank.asc()
        ).limit(10)
        
        result = await conn.execute(query)
        top_performers = result.fetchall()
        
        print("\nTop 10 Overall Performers (Weekly):")
        print("-" * 80)
        for row in top_performers:
            print(f"Rank {row.overall_rank:2d}: {row.miner_hotkey[:12]}... "
                  f"Score: {row.overall_avg_score:.3f} "
                  f"Tasks: {row.total_tasks_attempted:3d} "
                  f"Success: {row.overall_success_rate:.1%} "
                  f"Trend: {row.performance_trend}")

async def example_get_task_specific_rankings():
    """Example: Get task-specific performance rankings."""
    
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/gaia")
    
    async with engine.begin() as conn:
        # Get weather forecasting leaderboard
        weather_query = sa.select(
            miner_performance_stats_table.c.miner_hotkey,
            miner_performance_stats_table.c.weather_avg_score,
            miner_performance_stats_table.c.weather_rank,
            miner_performance_stats_table.c.weather_tasks_scored,
            miner_performance_stats_table.c.weather_best_score,
            miner_performance_stats_table.c.weather_latest_score
        ).where(
            sa.and_(
                miner_performance_stats_table.c.period_type == 'weekly',
                miner_performance_stats_table.c.weather_rank.isnot(None),
                miner_performance_stats_table.c.weather_tasks_scored > 0
            )
        ).order_by(
            miner_performance_stats_table.c.weather_rank.asc()
        ).limit(5)
        
        result = await conn.execute(weather_query)
        weather_leaders = result.fetchall()
        
        print("\nTop 5 Weather Forecasters:")
        print("-" * 60)
        for row in weather_leaders:
            print(f"Rank {row.weather_rank}: {row.miner_hotkey[:12]}... "
                  f"Avg: {row.weather_avg_score:.3f} "
                  f"Best: {row.weather_best_score:.3f} "
                  f"Latest: {row.weather_latest_score:.3f} "
                  f"({row.weather_tasks_scored} forecasts)")

async def example_bulk_analysis():
    """Example: Perform bulk analysis using numpy optimizations."""
    
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/gaia")
    
    async with engine.begin() as conn:
        calculator = MinerPerformanceCalculator(conn)
        
        # Get recent performance data
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=7)
        
        performances = await calculator.calculate_period_stats(
            period_type='weekly',
            period_start=start_time,
            period_end=end_time
        )
        
        if performances:
            # Calculate bulk rankings using optimized numpy operations
            rankings = await calculator.calculate_bulk_rankings(performances)
            
            print(f"\nBulk Analysis Results:")
            print(f"Total miners analyzed: {len(performances)}")
            print(f"Overall score percentiles: {rankings.get('overall_percentiles', [])}")
            
            # Extract score distributions
            all_scores = []
            for perf in performances:
                if perf.overall_avg_score is not None:
                    all_scores.append(np.array([perf.overall_avg_score]))
            
            if all_scores:
                distribution = calculator.batch_calculate_score_distributions(all_scores)
                print(f"Score distribution stats:")
                print(f"  Mean: {distribution.get('global_mean', 0):.3f}")
                print(f"  Std:  {distribution.get('global_std', 0):.3f}")
                print(f"  IQR:  {distribution.get('iqr', 0):.3f}")
                print(f"  Skew: {distribution.get('skewness', 0):.3f}")

async def example_monitoring_dashboard_data():
    """Example: Get data formatted for a monitoring dashboard."""
    
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/gaia")
    
    async with engine.begin() as conn:
        # Get comprehensive dashboard data
        query = sa.select(
            miner_performance_stats_table.c.miner_hotkey,
            miner_performance_stats_table.c.overall_avg_score,
            miner_performance_stats_table.c.overall_rank,
            miner_performance_stats_table.c.total_tasks_attempted,
            miner_performance_stats_table.c.total_tasks_completed,
            miner_performance_stats_table.c.overall_success_rate,
            miner_performance_stats_table.c.weather_avg_score,
            miner_performance_stats_table.c.soil_moisture_avg_score,
            miner_performance_stats_table.c.geomagnetic_avg_score,
            miner_performance_stats_table.c.performance_trend,
            miner_performance_stats_table.c.trend_confidence,
            miner_performance_stats_table.c.last_active_time,
            miner_performance_stats_table.c.score_distribution,
            miner_performance_stats_table.c.period_type,
            miner_performance_stats_table.c.calculated_at
        ).where(
            miner_performance_stats_table.c.period_type == 'daily'
        ).order_by(
            miner_performance_stats_table.c.calculated_at.desc(),
            miner_performance_stats_table.c.overall_rank.asc()
        )
        
        result = await conn.execute(query)
        dashboard_data = result.fetchall()
        
        print(f"\nDashboard Data Summary:")
        print(f"Total daily records: {len(dashboard_data)}")
        
        if dashboard_data:
            # Convert to format suitable for visualization
            active_miners = [row for row in dashboard_data if row.total_tasks_attempted > 0]
            
            print(f"Active miners: {len(active_miners)}")
            print(f"Avg success rate: {np.mean([row.overall_success_rate or 0 for row in active_miners]):.1%}")
            
            # Trend analysis
            trend_counts = {}
            for row in active_miners:
                trend = row.performance_trend or 'unknown'
                trend_counts[trend] = trend_counts.get(trend, 0) + 1
            
            print("Performance trends:")
            for trend, count in trend_counts.items():
                print(f"  {trend}: {count} miners")

if __name__ == "__main__":
    # Run examples
    print("Miner Performance Statistics Examples")
    print("=" * 50)
    
    asyncio.run(example_calculate_performance_stats())
    asyncio.run(example_query_performance_stats())
    asyncio.run(example_get_task_specific_rankings())
    asyncio.run(example_bulk_analysis())
    asyncio.run(example_monitoring_dashboard_data()) 