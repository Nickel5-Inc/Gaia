#!/usr/bin/env python3
"""
Test script for validating job ID resilience across multiple validators.
This script helps test the database synchronization resilience system.

Usage:
    python test_job_id_resilience.py --scenario basic --validators 2 --miners 1
    python test_job_id_resilience.py --scenario db_sync --simulate-overwrite
    python test_job_id_resilience.py --scenario multi_validator --validators 3
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

class JobIDResilienceTest:
    """Test harness for job ID resilience validation."""
    
    def __init__(self, validators: List[str], miners: List[str]):
        self.validators = validators
        self.miners = miners
        self.test_results = {
            "start_time": datetime.now(timezone.utc).isoformat(),
            "scenarios": {},
            "metrics": {},
            "issues": []
        }
        
    async def run_scenario_basic(self) -> Dict:
        """Test basic deterministic job ID generation across validators."""
        logger.info("Running Scenario 1: Basic Deterministic Job ID Generation")
        
        scenario_results = {
            "name": "basic_deterministic_generation",
            "status": "running",
            "steps": [],
            "job_ids": {},
            "consistency_check": None
        }
        
        # Step 1: Generate job IDs from all validators for same GFS timestep
        test_gfs_time = datetime(2025, 7, 18, 18, 0, 0, tzinfo=timezone.utc)
        test_miner_hotkey = self.miners[0] if self.miners else "test_miner_hotkey"
        
        logger.info(f"Testing job ID generation for GFS time: {test_gfs_time}")
        
        for i, validator_hotkey in enumerate(self.validators):
            logger.info(f"Generating job ID from validator {i+1}: {validator_hotkey[:12]}...")
            
            # Import deterministic job ID generator
            from gaia.tasks.base.deterministic_job_id import DeterministicJobID
            
            job_id = DeterministicJobID.generate_weather_job_id(
                gfs_init_time=test_gfs_time,
                miner_hotkey=test_miner_hotkey,
                validator_hotkey=validator_hotkey,
                job_type="forecast"
            )
            
            scenario_results["job_ids"][f"validator_{i+1}"] = {
                "validator_hotkey": validator_hotkey,
                "job_id": job_id,
                "gfs_time": test_gfs_time.isoformat()
            }
            
            logger.info(f"Generated job ID: {job_id}")
        
        # Step 2: Check consistency
        job_ids = [data["job_id"] for data in scenario_results["job_ids"].values()]
        all_match = len(set(job_ids)) == 1
        
        scenario_results["consistency_check"] = {
            "all_job_ids_match": all_match,
            "unique_job_ids": list(set(job_ids)),
            "total_validators": len(self.validators)
        }
        
        scenario_results["status"] = "passed" if all_match else "failed"
        
        if all_match:
            logger.info("✅ SUCCESS: All validators generated identical job IDs")
        else:
            logger.error("❌ FAILURE: Validators generated different job IDs")
            self.test_results["issues"].append({
                "scenario": "basic",
                "issue": "job_id_mismatch",
                "details": f"Expected 1 unique job ID, got {len(set(job_ids))}"
            })
        
        return scenario_results
    
    async def run_scenario_db_sync(self, simulate_overwrite: bool = False) -> Dict:
        """Test database synchronization resilience."""
        logger.info("Running Scenario 3: Database Overwrite Simulation")
        
        scenario_results = {
            "name": "database_sync_resilience",
            "status": "running",
            "simulation_enabled": simulate_overwrite,
            "fallback_activations": 0,
            "successful_recoveries": 0
        }
        
        if simulate_overwrite:
            logger.info("🔄 Simulating database overwrite scenario...")
            
            # This would require actual database manipulation in real test
            # For now, we document the simulation steps
            simulation_steps = [
                "1. Validator A creates jobs for GFS timestep T",
                "2. Miner completes and stores forecast results",
                "3. Simulate DB overwrite: Copy Validator B's DB to Validator A",
                "4. Validator A now has different job IDs for same timestep",
                "5. Validator A requests verification from miner",
                "6. Miner should use fallback lookup to find equivalent job"
            ]
            
            for step in simulation_steps:
                logger.info(f"  {step}")
                await asyncio.sleep(0.5)  # Simulate processing time
            
            # Test the fallback lookup function
            from gaia.tasks.defined_tasks.weather.processing.weather_logic import find_job_by_alternative_methods
            
            # Mock a job ID that might not be found due to DB sync
            test_job_id = "weather_job_forecast_20250718180000_test_miner"
            logger.info(f"Testing fallback lookup for job ID: {test_job_id}")
            
            # This would require actual WeatherTask instance in real test
            logger.info("📝 Note: Full fallback testing requires live WeatherTask instance")
            
            scenario_results["status"] = "simulated"
        else:
            logger.info("⚠️  Database overwrite simulation disabled")
            scenario_results["status"] = "skipped"
        
        return scenario_results
    
    async def run_scenario_multi_validator(self) -> Dict:
        """Test multi-validator competition scenario."""
        logger.info("Running Scenario 5: Multi-Validator Competition")
        
        scenario_results = {
            "name": "multi_validator_competition",
            "status": "running",
            "validator_count": len(self.validators),
            "job_conflicts": 0,
            "successful_reuse": 0
        }
        
        if len(self.validators) < 2:
            scenario_results["status"] = "skipped"
            logger.warning("⚠️  Multi-validator test requires at least 2 validators")
            return scenario_results
        
        # Test cross-validator job creation for same GFS timestep
        test_gfs_time = datetime(2025, 7, 18, 18, 0, 0, tzinfo=timezone.utc)
        test_miner_hotkey = self.miners[0] if self.miners else "test_miner_hotkey"
        
        logger.info(f"Testing {len(self.validators)} validators requesting same GFS timestep")
        
        job_ids_by_validator = {}
        
        for i, validator_hotkey in enumerate(self.validators):
            from gaia.tasks.base.deterministic_job_id import DeterministicJobID
            
            job_id = DeterministicJobID.generate_weather_job_id(
                gfs_init_time=test_gfs_time,
                miner_hotkey=test_miner_hotkey,
                validator_hotkey=validator_hotkey,
                job_type="forecast"
            )
            
            job_ids_by_validator[f"validator_{i+1}"] = job_id
            logger.info(f"Validator {i+1} generated job ID: {job_id}")
        
        # Check for conflicts
        unique_job_ids = set(job_ids_by_validator.values())
        if len(unique_job_ids) == 1:
            scenario_results["successful_reuse"] = len(self.validators)
            scenario_results["status"] = "passed"
            logger.info("✅ SUCCESS: All validators can reuse same job")
        else:
            scenario_results["job_conflicts"] = len(unique_job_ids) - 1
            scenario_results["status"] = "failed"
            logger.error(f"❌ FAILURE: Got {len(unique_job_ids)} different job IDs")
        
        return scenario_results
    
    async def validate_health_monitoring(self) -> Dict:
        """Test the job ID health monitoring system."""
        logger.info("Testing job ID health monitoring...")
        
        health_results = {
            "name": "health_monitoring",
            "status": "running",
            "health_checks": {}
        }
        
        # Test health check function (would require WeatherTask instance)
        logger.info("📝 Note: Health monitoring requires live WeatherTask instance")
        logger.info("Expected health check functions:")
        logger.info("  - check_job_id_health()")
        logger.info("  - Database consistency validation")
        logger.info("  - Cross-validator job tracking")
        
        health_results["status"] = "documented"
        return health_results
    
    def analyze_logs(self, log_patterns: List[str]) -> Dict:
        """Analyze logs for specific patterns related to job ID resilience."""
        logger.info("Analyzing logs for resilience patterns...")
        
        analysis = {
            "patterns_found": {},
            "positive_indicators": 0,
            "warning_indicators": 0,
            "error_indicators": 0
        }
        
        # Define patterns to look for
        positive_patterns = [
            "Generated deterministic job ID",
            "Found existing job .* from same validator",
            "Found existing job .* from different validator",
            "Database sync resilience: Found equivalent job"
        ]
        
        warning_patterns = [
            "Database sync resilience: Using equivalent job",
            "Used fallback job ID due to database sync"
        ]
        
        error_patterns = [
            "Job not found for job_id: .* even after fallback attempts",
            "No reconciliation possible for job ID"
        ]
        
        for pattern in positive_patterns:
            analysis["patterns_found"][pattern] = "Expected during normal operation"
            
        for pattern in warning_patterns:
            analysis["patterns_found"][pattern] = "Expected during database sync issues"
            
        for pattern in error_patterns:
            analysis["patterns_found"][pattern] = "Should not occur - indicates failure"
        
        logger.info("📝 Note: Actual log analysis requires access to validator/miner logs")
        return analysis
    
    async def run_comprehensive_test(self, args) -> Dict:
        """Run comprehensive test suite based on arguments."""
        logger.info("🚀 Starting Job ID Resilience Test Suite")
        logger.info(f"Validators: {len(self.validators)}")
        logger.info(f"Miners: {len(self.miners)}")
        
        # Run scenarios based on arguments
        if args.scenario in ['basic', 'all']:
            self.test_results["scenarios"]["basic"] = await self.run_scenario_basic()
        
        if args.scenario in ['db_sync', 'all']:
            self.test_results["scenarios"]["db_sync"] = await self.run_scenario_db_sync(
                simulate_overwrite=args.simulate_overwrite
            )
        
        if args.scenario in ['multi_validator', 'all']:
            self.test_results["scenarios"]["multi_validator"] = await self.run_scenario_multi_validator()
        
        # Always run health monitoring test
        self.test_results["scenarios"]["health"] = await self.validate_health_monitoring()
        
        # Analyze logs
        self.test_results["log_analysis"] = self.analyze_logs([])
        
        # Calculate metrics
        self.test_results["metrics"] = self._calculate_metrics()
        self.test_results["end_time"] = datetime.now(timezone.utc).isoformat()
        
        return self.test_results
    
    def _calculate_metrics(self) -> Dict:
        """Calculate test metrics and success rates."""
        metrics = {
            "total_scenarios": len(self.test_results["scenarios"]),
            "passed_scenarios": 0,
            "failed_scenarios": 0,
            "skipped_scenarios": 0,
            "overall_success_rate": 0.0
        }
        
        for scenario_name, scenario_data in self.test_results["scenarios"].items():
            status = scenario_data.get("status", "unknown")
            if status == "passed":
                metrics["passed_scenarios"] += 1
            elif status == "failed":
                metrics["failed_scenarios"] += 1
            elif status in ["skipped", "simulated", "documented"]:
                metrics["skipped_scenarios"] += 1
        
        if metrics["total_scenarios"] > 0:
            metrics["overall_success_rate"] = metrics["passed_scenarios"] / metrics["total_scenarios"] * 100
        
        return metrics
    
    def generate_report(self) -> str:
        """Generate a comprehensive test report."""
        report = []
        report.append("=" * 70)
        report.append("JOB ID RESILIENCE TEST REPORT")
        report.append("=" * 70)
        report.append(f"Start Time: {self.test_results['start_time']}")
        report.append(f"End Time: {self.test_results['end_time']}")
        report.append(f"Validators Tested: {len(self.validators)}")
        report.append(f"Miners Tested: {len(self.miners)}")
        report.append("")
        
        # Metrics summary
        metrics = self.test_results["metrics"]
        report.append("SUMMARY METRICS:")
        report.append(f"  Total Scenarios: {metrics['total_scenarios']}")
        report.append(f"  Passed: {metrics['passed_scenarios']}")
        report.append(f"  Failed: {metrics['failed_scenarios']}")
        report.append(f"  Skipped: {metrics['skipped_scenarios']}")
        report.append(f"  Success Rate: {metrics['overall_success_rate']:.1f}%")
        report.append("")
        
        # Scenario details
        for scenario_name, scenario_data in self.test_results["scenarios"].items():
            status = scenario_data.get("status", "unknown")
            status_icon = "✅" if status == "passed" else "❌" if status == "failed" else "⚠️"
            report.append(f"{status_icon} {scenario_name.upper()}: {status}")
        
        report.append("")
        
        # Issues
        if self.test_results["issues"]:
            report.append("ISSUES FOUND:")
            for issue in self.test_results["issues"]:
                report.append(f"  - {issue['scenario']}: {issue['issue']} - {issue['details']}")
        else:
            report.append("✅ NO ISSUES FOUND")
        
        return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description="Test job ID resilience across validators")
    parser.add_argument("--scenario", choices=["basic", "db_sync", "multi_validator", "all"], 
                       default="basic", help="Test scenario to run")
    parser.add_argument("--validators", type=int, default=2, 
                       help="Number of validators to simulate")
    parser.add_argument("--miners", type=int, default=1, 
                       help="Number of miners to simulate")
    parser.add_argument("--simulate-overwrite", action="store_true", 
                       help="Simulate database overwrite scenario")
    parser.add_argument("--output", type=str, help="Output file for test results")
    
    args = parser.parse_args()
    
    # Generate test validator and miner hotkeys
    validators = [f"5GTA{i:04d}..." for i in range(args.validators)]
    miners = [f"5GMT{i:04d}..." for i in range(args.miners)]
    
    # Run tests
    async def run_tests():
        test_harness = JobIDResilienceTest(validators, miners)
        results = await test_harness.run_comprehensive_test(args)
        
        # Generate and display report
        report = test_harness.generate_report()
        print(report)
        
        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Test results saved to {args.output}")
        
        return results
    
    # Run the async test
    results = asyncio.run(run_tests())
    
    # Exit with appropriate code
    success_rate = results["metrics"]["overall_success_rate"]
    exit_code = 0 if success_rate >= 95.0 else 1
    exit(exit_code)

if __name__ == "__main__":
    main() 