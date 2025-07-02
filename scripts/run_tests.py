#!/usr/bin/env python3
"""
Comprehensive test runner for Gaia Validator v4.0 Phase 4 testing.

This script orchestrates different types of tests:
- Unit tests with coverage
- Integration tests  
- Performance benchmarks
- Soak tests

Usage:
    python scripts/run_tests.py --unit                    # Run unit tests only
    python scripts/run_tests.py --integration             # Run integration tests only  
    python scripts/run_tests.py --benchmark              # Run performance benchmarks
    python scripts/run_tests.py --soak --duration=72h    # Run 72-hour soak test
    python scripts/run_tests.py --all                    # Run all tests (except soak)
"""

import argparse
import subprocess
import sys
import os
import time
from pathlib import Path
import json
from datetime import datetime


def run_command(cmd, cwd=None, capture_output=False):
    """Run a shell command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    
    try:
        if capture_output:
            result = subprocess.run(
                cmd, 
                cwd=cwd, 
                capture_output=True, 
                text=True,
                check=True
            )
            return result.stdout
        else:
            result = subprocess.run(cmd, cwd=cwd, check=True)
            return None
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        if capture_output and e.stderr:
            print(f"Error output: {e.stderr}")
        return None


def check_dependencies():
    """Check if required dependencies are installed."""
    print("Checking test dependencies...")
    
    required_packages = [
        "pytest",
        "pytest-cov", 
        "pytest-benchmark",
        "pytest-asyncio",
        "locust"
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"Missing required packages: {', '.join(missing)}")
        print("Install with: pip install -r requirements-test.txt")
        return False
    
    print("✓ All test dependencies found")
    return True


def run_unit_tests(coverage=True, parallel=True):
    """Run unit tests with optional coverage and parallelization."""
    print("\n=== Running Unit Tests ===")
    
    cmd = ["python", "-m", "pytest", "tests/unit/", "-v"]
    
    if coverage:
        cmd.extend([
            "--cov=gaia/validator",
            "--cov=gaia/tasks/defined_tasks/weather/validator",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-report=xml",
            "--cov-fail-under=90"
        ])
    
    if parallel:
        # Add parallel execution if pytest-xdist is available
        try:
            import xdist
            cmd.extend(["-n", "auto"])
        except ImportError:
            print("pytest-xdist not available, running tests sequentially")
    
    # Add markers to run only unit tests
    cmd.extend(["-m", "unit"])
    
    success = run_command(cmd) is not None
    
    if success:
        print("✓ Unit tests passed")
        if coverage:
            print("Coverage report generated in htmlcov/")
    else:
        print("✗ Unit tests failed")
    
    return success


def run_integration_tests():
    """Run integration tests."""
    print("\n=== Running Integration Tests ===")
    
    # Check if Docker is available for test database
    docker_available = run_command(
        ["docker", "--version"], 
        capture_output=True
    ) is not None
    
    if docker_available:
        print("Docker available - starting test database")
        
        # Start test database
        run_command([
            "docker-compose", 
            "-f", "docker-compose.test.yml", 
            "up", "-d", "test-postgres"
        ])
        
        # Wait for database to be ready
        print("Waiting for test database to be ready...")
        time.sleep(10)
        
        try:
            # Run integration tests
            cmd = [
                "python", "-m", "pytest", 
                "tests/integration/", 
                "-v", 
                "-m", "integration",
                "--tb=short"
            ]
            
            success = run_command(cmd) is not None
            
        finally:
            # Clean up test database
            print("Cleaning up test database...")
            run_command([
                "docker-compose", 
                "-f", "docker-compose.test.yml", 
                "down", "-v"
            ])
    else:
        print("Docker not available - running integration tests with mocked database")
        
        cmd = [
            "python", "-m", "pytest", 
            "tests/integration/", 
            "-v", 
            "-m", "integration",
            "--tb=short"
        ]
        
        success = run_command(cmd) is not None
    
    if success:
        print("✓ Integration tests passed")
    else:
        print("✗ Integration tests failed")
    
    return success


def run_performance_benchmarks():
    """Run performance benchmarks."""
    print("\n=== Running Performance Benchmarks ===")
    
    cmd = [
        "python", "-m", "pytest", 
        "tests/performance/",
        "-v",
        "-m", "benchmark", 
        "--benchmark-only",
        "--benchmark-sort=mean",
        "--benchmark-columns=min,max,mean,stddev,median,iqr,outliers,ops,rounds",
        "--benchmark-json=benchmark_results.json"
    ]
    
    success = run_command(cmd) is not None
    
    if success:
        print("✓ Performance benchmarks completed")
        print("Benchmark results saved to benchmark_results.json")
        
        # Display summary if results file exists
        if os.path.exists("benchmark_results.json"):
            display_benchmark_summary()
    else:
        print("✗ Performance benchmarks failed")
    
    return success


def display_benchmark_summary():
    """Display a summary of benchmark results."""
    try:
        with open("benchmark_results.json", "r") as f:
            data = json.load(f)
        
        print("\n=== Benchmark Summary ===")
        
        for benchmark in data.get("benchmarks", []):
            name = benchmark.get("name", "Unknown")
            stats = benchmark.get("stats", {})
            mean = stats.get("mean", 0)
            min_time = stats.get("min", 0)
            max_time = stats.get("max", 0)
            
            print(f"{name:50} | Mean: {mean*1000:.2f}ms | Min: {min_time*1000:.2f}ms | Max: {max_time*1000:.2f}ms")
        
        print("=" * 100)
        
    except Exception as e:
        print(f"Could not display benchmark summary: {e}")


def run_soak_test(duration="30m", users=20):
    """Run soak tests using Locust."""
    print(f"\n=== Running Soak Test (Duration: {duration}, Users: {users}) ===")
    
    # Check if application is running
    app_running = check_application_health()
    
    if not app_running:
        print("Warning: Application may not be running. Starting in test mode...")
        # In a real scenario, you might want to start the application here
    
    # Run Locust soak test
    cmd = [
        "locust",
        "-f", "tests/soak/locustfile.py",
        "--headless",
        f"--users={users}",
        "--spawn-rate=2",
        f"--run-time={duration}",
        "--host=http://localhost:8000",
        "--html=soak_test_report.html",
        "--csv=soak_test_results"
    ]
    
    print(f"Starting soak test with {users} users for {duration}")
    print("This will generate load against the application...")
    
    success = run_command(cmd) is not None
    
    if success:
        print("✓ Soak test completed")
        print("Report generated: soak_test_report.html")
        analyze_soak_test_results()
    else:
        print("✗ Soak test failed")
    
    return success


def check_application_health():
    """Check if the application is running and healthy."""
    try:
        import requests
        response = requests.get("http://localhost:8000/health", timeout=5)
        return response.status_code == 200
    except:
        return False


def analyze_soak_test_results():
    """Analyze soak test results against success criteria."""
    print("\n=== Soak Test Analysis ===")
    
    # Success criteria from the blueprint
    criteria = {
        "max_failure_rate": 0.05,  # 5%
        "max_avg_response_time": 2000,  # 2000ms
        "max_p99_response_time": 5000,  # 5000ms
    }
    
    try:
        # Read Locust results
        if os.path.exists("soak_test_results_stats.csv"):
            import pandas as pd
            
            stats = pd.read_csv("soak_test_results_stats.csv")
            
            # Calculate metrics
            total_requests = stats["Request Count"].sum()
            total_failures = stats["Failure Count"].sum()
            failure_rate = total_failures / max(total_requests, 1)
            avg_response_time = stats["Average Response Time"].mean()
            p99_response_time = stats["99%"].mean()
            
            print(f"Total Requests: {total_requests:,}")
            print(f"Total Failures: {total_failures:,}")
            print(f"Failure Rate: {failure_rate*100:.2f}% (target: <{criteria['max_failure_rate']*100:.1f}%)")
            print(f"Average Response Time: {avg_response_time:.2f}ms (target: <{criteria['max_avg_response_time']}ms)")
            print(f"99th Percentile Response Time: {p99_response_time:.2f}ms (target: <{criteria['max_p99_response_time']}ms)")
            
            # Check success criteria
            criteria_met = 0
            total_criteria = len(criteria)
            
            if failure_rate <= criteria["max_failure_rate"]:
                print("✓ Failure rate criterion MET")
                criteria_met += 1
            else:
                print("✗ Failure rate criterion FAILED")
            
            if avg_response_time <= criteria["max_avg_response_time"]:
                print("✓ Average response time criterion MET")
                criteria_met += 1
            else:
                print("✗ Average response time criterion FAILED")
            
            if p99_response_time <= criteria["max_p99_response_time"]:
                print("✓ 99th percentile response time criterion MET")
                criteria_met += 1
            else:
                print("✗ 99th percentile response time criterion FAILED")
            
            print(f"\nOverall: {criteria_met}/{total_criteria} criteria met")
            
            if criteria_met == total_criteria:
                print("🎉 SOAK TEST PASSED - All criteria met!")
                return True
            else:
                print("❌ SOAK TEST FAILED - Some criteria not met")
                return False
        
    except Exception as e:
        print(f"Could not analyze soak test results: {e}")
        return False


def run_compilation_checks():
    """Run compilation checks on all Python files."""
    print("\n=== Running Compilation Checks ===")
    
    python_files = []
    
    # Find all Python files
    for root in ["gaia/", "tests/"]:
        if os.path.exists(root):
            for path in Path(root).rglob("*.py"):
                python_files.append(str(path))
    
    print(f"Checking {len(python_files)} Python files...")
    
    failed_files = []
    
    for file_path in python_files:
        try:
            cmd = ["python", "-m", "py_compile", file_path]
            result = run_command(cmd, capture_output=True)
            if result is None:
                failed_files.append(file_path)
        except:
            failed_files.append(file_path)
    
    if failed_files:
        print(f"✗ Compilation failed for {len(failed_files)} files:")
        for file_path in failed_files:
            print(f"  - {file_path}")
        return False
    else:
        print("✓ All Python files compile successfully")
        return True


def generate_test_report(results):
    """Generate a comprehensive test report."""
    print("\n=== Test Report ===")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report = {
        "timestamp": timestamp,
        "test_results": results,
        "summary": {
            "total_test_suites": len(results),
            "passed_suites": sum(1 for r in results.values() if r),
            "failed_suites": sum(1 for r in results.values() if not r)
        }
    }
    
    # Save report to file
    with open("test_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"Test Report Generated: {timestamp}")
    print(f"Total Test Suites: {report['summary']['total_test_suites']}")
    print(f"Passed: {report['summary']['passed_suites']}")
    print(f"Failed: {report['summary']['failed_suites']}")
    
    for suite, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {suite}: {status}")
    
    print(f"\nDetailed report saved to: test_report.json")
    
    # Return overall success
    return report['summary']['failed_suites'] == 0


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="Gaia Validator v4.0 Test Runner")
    parser.add_argument("--unit", action="store_true", help="Run unit tests")
    parser.add_argument("--integration", action="store_true", help="Run integration tests")
    parser.add_argument("--benchmark", action="store_true", help="Run performance benchmarks")
    parser.add_argument("--soak", action="store_true", help="Run soak tests")
    parser.add_argument("--all", action="store_true", help="Run all tests except soak")
    parser.add_argument("--duration", default="30m", help="Soak test duration (default: 30m)")
    parser.add_argument("--users", type=int, default=20, help="Number of soak test users (default: 20)")
    parser.add_argument("--no-coverage", action="store_true", help="Skip coverage reporting")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel test execution")
    
    args = parser.parse_args()
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    results = {}
    
    # Compilation checks always run
    results["compilation"] = run_compilation_checks()
    
    # Determine which tests to run
    if args.all:
        args.unit = True
        args.integration = True
        args.benchmark = True
    
    if not any([args.unit, args.integration, args.benchmark, args.soak]):
        # Default to unit tests if nothing specified
        args.unit = True
    
    # Run selected test suites
    if args.unit:
        results["unit_tests"] = run_unit_tests(
            coverage=not args.no_coverage,
            parallel=not args.no_parallel
        )
    
    if args.integration:
        results["integration_tests"] = run_integration_tests()
    
    if args.benchmark:
        results["performance_benchmarks"] = run_performance_benchmarks()
    
    if args.soak:
        results["soak_tests"] = run_soak_test(args.duration, args.users)
    
    # Generate final report
    overall_success = generate_test_report(results)
    
    if overall_success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()