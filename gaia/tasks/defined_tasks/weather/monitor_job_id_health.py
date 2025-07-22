#!/usr/bin/env python3
"""
Job ID Health Monitoring Script

This script continuously monitors job ID health and database synchronization
issues in a live Gaia weather task environment.

Usage:
    python monitor_job_id_health.py --node-type validator --interval 300
    python monitor_job_id_health.py --node-type miner --output health_report.json
"""

import asyncio
import argparse
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class JobIDHealthMonitor:
    """Monitors job ID health and database synchronization issues."""
    
    def __init__(self, node_type: str, monitor_interval: int = 300):
        self.node_type = node_type
        self.monitor_interval = monitor_interval
        self.health_history = []
        self.alerts_sent = set()
        
    async def check_health(self, weather_task=None) -> Dict:
        """Perform health check and return results."""
        timestamp = datetime.now(timezone.utc)
        
        health_data = {
            "timestamp": timestamp.isoformat(),
            "node_type": self.node_type,
            "status": "unknown",
            "metrics": {},
            "issues": [],
            "recommendations": []
        }
        
        if weather_task:
            # Use the actual health check function
            from gaia.tasks.defined_tasks.weather.processing.weather_logic import check_job_id_health
            try:
                health_report = await check_job_id_health(weather_task)
                health_data.update(health_report)
                health_data["status"] = "checked"
                logger.info(f"Health check completed: {health_data['status']}")
            except Exception as e:
                health_data["status"] = "error"
                health_data["issues"].append({
                    "type": "health_check_error",
                    "message": str(e)
                })
                logger.error(f"Health check failed: {e}")
        else:
            # Simulate health check for testing
            health_data.update(self._simulate_health_check())
            
        return health_data
    
    def _simulate_health_check(self) -> Dict:
        """Simulate health check for testing purposes."""
        logger.info(f"Simulating health check for {self.node_type} node...")
        
        # Simulate different health states
        import random
        
        base_data = {
            "status": "simulated",
            "metrics": {},
            "issues": [],
            "recommendations": []
        }
        
        if self.node_type == "miner":
            # Simulate miner health metrics
            base_data["metrics"] = {
                "total_jobs_24h": random.randint(10, 50),
                "unique_validators_24h": random.randint(1, 3),
                "job_reuse_count": random.randint(5, 20)
            }
            
            # Occasionally simulate issues
            if random.random() < 0.3:  # 30% chance of issues
                base_data["issues"].append({
                    "type": "multiple_validators",
                    "description": f"Jobs from {base_data['metrics']['unique_validators_24h']} different validators",
                    "impact": "Potential for job ID conflicts during database sync"
                })
                base_data["recommendations"].append("Monitor for database sync issues")
                
        elif self.node_type == "validator":
            # Simulate validator health metrics
            base_data["metrics"] = {
                "sync_issues_24h": random.randint(0, 5),
                "affected_miners_24h": random.randint(0, 3),
                "verification_success_rate": random.uniform(95.0, 99.9)
            }
            
            if base_data["metrics"]["sync_issues_24h"] > 0:
                base_data["issues"].append({
                    "type": "job_id_mismatches",
                    "count": base_data["metrics"]["sync_issues_24h"],
                    "description": "Job ID mismatches detected (database sync resilience activated)",
                    "impact": "Some jobs required fallback lookup methods"
                })
                base_data["recommendations"].append("Monitor database synchronization timing")
        
        return base_data
    
    def analyze_trends(self, lookback_hours: int = 24) -> Dict:
        """Analyze health trends over time."""
        if not self.health_history:
            return {"status": "no_data", "message": "No historical data available"}
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        recent_health = [
            h for h in self.health_history 
            if datetime.fromisoformat(h["timestamp"]) > cutoff_time
        ]
        
        if not recent_health:
            return {"status": "no_recent_data", "message": f"No data in last {lookback_hours} hours"}
        
        analysis = {
            "period_hours": lookback_hours,
            "total_checks": len(recent_health),
            "health_distribution": {},
            "issue_trends": {},
            "recommendations": []
        }
        
        # Analyze health status distribution
        status_counts = {}
        for health in recent_health:
            status = health.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        
        total_checks = len(recent_health)
        analysis["health_distribution"] = {
            status: {"count": count, "percentage": (count / total_checks) * 100}
            for status, count in status_counts.items()
        }
        
        # Analyze issue trends
        issue_types = {}
        for health in recent_health:
            for issue in health.get("issues", []):
                issue_type = issue.get("type", "unknown")
                issue_types[issue_type] = issue_types.get(issue_type, 0) + 1
        
        analysis["issue_trends"] = issue_types
        
        # Generate recommendations based on trends
        if issue_types.get("job_id_mismatches", 0) > 0:
            analysis["recommendations"].append(
                "Database sync issues detected - consider monitoring sync timing"
            )
        
        if issue_types.get("multiple_validators", 0) > 0:
            analysis["recommendations"].append(
                "Multiple validators detected - ensure resilience features are enabled"
            )
        
        error_rate = status_counts.get("error", 0) / total_checks * 100
        if error_rate > 10:
            analysis["recommendations"].append(
                f"High error rate ({error_rate:.1f}%) - investigate health check failures"
            )
        
        return analysis
    
    def check_alert_conditions(self, health_data: Dict) -> List[Dict]:
        """Check if any alert conditions are met."""
        alerts = []
        
        # Alert on high error rates
        if health_data["status"] == "error":
            alert_key = f"health_check_error_{datetime.now().hour}"
            if alert_key not in self.alerts_sent:
                alerts.append({
                    "level": "error",
                    "type": "health_check_failure",
                    "message": "Health check is failing",
                    "details": health_data.get("issues", [])
                })
                self.alerts_sent.add(alert_key)
        
        # Alert on database sync issues (validator only)
        if self.node_type == "validator":
            sync_issues = health_data.get("metrics", {}).get("sync_issues_24h", 0)
            if sync_issues > 10:  # More than 10 sync issues in 24h
                alert_key = f"high_sync_issues_{datetime.now().date()}"
                if alert_key not in self.alerts_sent:
                    alerts.append({
                        "level": "warning",
                        "type": "high_sync_issues",
                        "message": f"High number of database sync issues: {sync_issues}",
                        "details": {"sync_issues_24h": sync_issues}
                    })
                    self.alerts_sent.add(alert_key)
        
        # Alert on multiple validators (miner only)
        if self.node_type == "miner":
            unique_validators = health_data.get("metrics", {}).get("unique_validators_24h", 0)
            if unique_validators > 3:  # More than 3 different validators
                alert_key = f"many_validators_{datetime.now().date()}"
                if alert_key not in self.alerts_sent:
                    alerts.append({
                        "level": "info",
                        "type": "multiple_validators",
                        "message": f"Receiving jobs from {unique_validators} different validators",
                        "details": {"unique_validators_24h": unique_validators}
                    })
                    self.alerts_sent.add(alert_key)
        
        return alerts
    
    async def run_continuous_monitoring(self, weather_task=None, output_file=None):
        """Run continuous health monitoring."""
        logger.info(f"Starting continuous job ID health monitoring for {self.node_type}")
        logger.info(f"Monitoring interval: {self.monitor_interval} seconds")
        
        try:
            while True:
                # Perform health check
                health_data = await self.check_health(weather_task)
                self.health_history.append(health_data)
                
                # Check for alerts
                alerts = self.check_alert_conditions(health_data)
                for alert in alerts:
                    logger.warning(f"ALERT: {alert['message']}")
                
                # Log current status
                status = health_data["status"]
                issues_count = len(health_data.get("issues", []))
                logger.info(f"Health Status: {status}, Issues: {issues_count}")
                
                # Periodic trend analysis
                if len(self.health_history) % 12 == 0:  # Every 12 checks (1 hour if 5min intervals)
                    trends = self.analyze_trends()
                    logger.info(f"Trends: {trends.get('health_distribution', {})}")
                
                # Save to file if requested
                if output_file:
                    self.save_health_data(output_file)
                
                # Clean up old history (keep last 7 days)
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
                self.health_history = [
                    h for h in self.health_history 
                    if datetime.fromisoformat(h["timestamp"]) > cutoff_time
                ]
                
                # Wait for next check
                await asyncio.sleep(self.monitor_interval)
                
        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
    
    def save_health_data(self, output_file: str):
        """Save health data to file."""
        try:
            data = {
                "node_type": self.node_type,
                "monitor_interval": self.monitor_interval,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "health_history": self.health_history[-100:],  # Keep last 100 entries
                "trends": self.analyze_trends()
            }
            
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save health data: {e}")
    
    def generate_summary_report(self) -> str:
        """Generate a summary report of current health status."""
        if not self.health_history:
            return "No health data available"
        
        latest = self.health_history[-1]
        trends = self.analyze_trends()
        
        report = []
        report.append("=" * 50)
        report.append("JOB ID HEALTH MONITORING SUMMARY")
        report.append("=" * 50)
        report.append(f"Node Type: {self.node_type}")
        report.append(f"Last Check: {latest['timestamp']}")
        report.append(f"Current Status: {latest['status']}")
        report.append(f"Total Checks: {len(self.health_history)}")
        report.append("")
        
        # Current metrics
        if latest.get("metrics"):
            report.append("CURRENT METRICS:")
            for key, value in latest["metrics"].items():
                report.append(f"  {key}: {value}")
            report.append("")
        
        # Current issues
        if latest.get("issues"):
            report.append("CURRENT ISSUES:")
            for issue in latest["issues"]:
                report.append(f"  - {issue.get('type', 'unknown')}: {issue.get('description', 'No description')}")
            report.append("")
        
        # Trends
        if trends.get("health_distribution"):
            report.append("HEALTH TRENDS (24h):")
            for status, data in trends["health_distribution"].items():
                report.append(f"  {status}: {data['count']} checks ({data['percentage']:.1f}%)")
            report.append("")
        
        # Recommendations
        if latest.get("recommendations") or trends.get("recommendations"):
            report.append("RECOMMENDATIONS:")
            all_recommendations = set(latest.get("recommendations", []) + trends.get("recommendations", []))
            for rec in all_recommendations:
                report.append(f"  - {rec}")
        
        return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description="Monitor job ID health in live system")
    parser.add_argument("--node-type", choices=["validator", "miner"], required=True,
                       help="Type of node to monitor")
    parser.add_argument("--interval", type=int, default=300,
                       help="Monitoring interval in seconds (default: 300)")
    parser.add_argument("--output", type=str,
                       help="Output file for health data (JSON)")
    parser.add_argument("--one-shot", action="store_true",
                       help="Run single health check instead of continuous monitoring")
    parser.add_argument("--report", action="store_true",
                       help="Generate summary report and exit")
    
    args = parser.parse_args()
    
    monitor = JobIDHealthMonitor(args.node_type, args.interval)
    
    if args.report and args.output and Path(args.output).exists():
        # Load existing data and generate report
        try:
            with open(args.output, 'r') as f:
                data = json.load(f)
                monitor.health_history = data.get("health_history", [])
            
            report = monitor.generate_summary_report()
            print(report)
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
        return
    
    async def run_monitoring():
        if args.one_shot:
            # Single health check
            health_data = await monitor.check_health()
            print(json.dumps(health_data, indent=2))
            
            if args.output:
                monitor.health_history.append(health_data)
                monitor.save_health_data(args.output)
        else:
            # Continuous monitoring
            await monitor.run_continuous_monitoring(
                weather_task=None,  # Would need actual WeatherTask instance
                output_file=args.output
            )
    
    # Run the monitoring
    try:
        asyncio.run(run_monitoring())
    except KeyboardInterrupt:
        logger.info("Monitoring stopped")

if __name__ == "__main__":
    main() 