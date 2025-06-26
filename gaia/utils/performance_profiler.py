"""
Live Performance Profiler for Gaia Validator
Provides multiple profiling approaches for identifying performance hotspots
"""

import os
import sys
import time
import cProfile
import pstats
import traceback
import threading
import asyncio
import functools
from typing import Dict, List, Any, Optional, Callable
from contextlib import contextmanager
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class LiveProfiler:
    """
    Live performance profiler that can be enabled/disabled via environment variables
    """
    
    def __init__(self):
        self.enabled = os.getenv('ENABLE_PROFILING', 'false').lower() == 'true'
        self.profile_mode = os.getenv('PROFILE_MODE', 'hotspots').lower()  # hotspots, detailed, memory
        self.profile_interval = int(os.getenv('PROFILE_INTERVAL_SECONDS', '60'))
        self.output_dir = os.getenv('PROFILE_OUTPUT_DIR', './profiling_output')
        
        # Performance tracking
        self.function_times = defaultdict(list)
        self.slow_functions = defaultdict(int)
        self.call_counts = defaultdict(int)
        self.lock = threading.Lock()
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        if self.enabled:
            logger.info(f"🔍 Live profiler enabled - mode: {self.profile_mode}, interval: {self.profile_interval}s")
            logger.info(f"📊 Profile output directory: {self.output_dir}")
        
    def profile_function(self, threshold_seconds=0.1):
        """
        Decorator to profile individual functions
        Usage: @profiler.profile_function(threshold_seconds=0.5)
        """
        def decorator(func):
            if not self.enabled:
                return func
                
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    function_name = f"{func.__module__}.{func.__name__}"
                    
                    with self.lock:
                        self.function_times[function_name].append(execution_time)
                        self.call_counts[function_name] += 1
                        
                        if execution_time > threshold_seconds:
                            self.slow_functions[function_name] += 1
                            logger.warning(f"🐌 Slow function detected: {function_name} took {execution_time:.3f}s")
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    return result
                finally:
                    execution_time = time.time() - start_time
                    function_name = f"{func.__module__}.{func.__name__}"
                    
                    with self.lock:
                        self.function_times[function_name].append(execution_time)
                        self.call_counts[function_name] += 1
                        
                        if execution_time > threshold_seconds:
                            self.slow_functions[function_name] += 1
                            logger.warning(f"🐌 Slow async function detected: {function_name} took {execution_time:.3f}s")
            
            return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
        return decorator
    
    @contextmanager
    def profile_block(self, block_name: str, threshold_seconds=0.1):
        """
        Context manager to profile code blocks
        Usage: 
        with profiler.profile_block("database_query"):
            result = expensive_operation()
        """
        if not self.enabled:
            yield
            return
            
        start_time = time.time()
        try:
            yield
        finally:
            execution_time = time.time() - start_time
            with self.lock:
                self.function_times[block_name].append(execution_time)
                self.call_counts[block_name] += 1
                
                if execution_time > threshold_seconds:
                    self.slow_functions[block_name] += 1
                    logger.warning(f"🐌 Slow code block: {block_name} took {execution_time:.3f}s")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        with self.lock:
            summary = {
                'top_slow_functions': {},
                'most_called_functions': {},
                'average_times': {},
                'total_calls': sum(self.call_counts.values()),
                'unique_functions': len(self.function_times)
            }
            
            # Top slow functions by frequency
            for func_name, count in sorted(self.slow_functions.items(), 
                                         key=lambda x: x[1], reverse=True)[:10]:
                summary['top_slow_functions'][func_name] = count
            
            # Most called functions
            for func_name, count in sorted(self.call_counts.items(), 
                                         key=lambda x: x[1], reverse=True)[:10]:
                summary['most_called_functions'][func_name] = count
            
            # Average execution times
            for func_name, times in self.function_times.items():
                if times:
                    avg_time = sum(times) / len(times)
                    max_time = max(times)
                    summary['average_times'][func_name] = {
                        'avg': avg_time,
                        'max': max_time,
                        'calls': len(times)
                    }
            
            return summary
    
    def print_performance_report(self):
        """Print a detailed performance report"""
        if not self.enabled:
            return
            
        summary = self.get_performance_summary()
        
        logger.info("🔍 LIVE PERFORMANCE REPORT 🔍")
        logger.info("=" * 60)
        
        logger.info(f"Total function calls tracked: {summary['total_calls']}")
        logger.info(f"Unique functions tracked: {summary['unique_functions']}")
        
        logger.info("\n🐌 TOP SLOW FUNCTIONS (by frequency):")
        for func_name, count in list(summary['top_slow_functions'].items())[:5]:
            logger.info(f"  {count:3d}x slow: {func_name}")
        
        logger.info("\n📈 MOST CALLED FUNCTIONS:")
        for func_name, count in list(summary['most_called_functions'].items())[:5]:
            avg_data = summary['average_times'].get(func_name, {})
            avg_time = avg_data.get('avg', 0)
            logger.info(f"  {count:4d} calls: {func_name} (avg: {avg_time:.3f}s)")
        
        logger.info("\n⏱️  HIGHEST AVERAGE EXECUTION TIMES:")
        sorted_by_avg = sorted(summary['average_times'].items(), 
                              key=lambda x: x[1]['avg'], reverse=True)
        for func_name, data in sorted_by_avg[:5]:
            logger.info(f"  {data['avg']:.3f}s avg ({data['max']:.3f}s max, {data['calls']} calls): {func_name}")
        
        logger.info("=" * 60)
    
    def save_cprofile_snapshot(self, duration_seconds=30):
        """Save a detailed cProfile snapshot"""
        if not self.enabled:
            return
            
        timestamp = int(time.time())
        profile_file = os.path.join(self.output_dir, f"cprofile_{timestamp}.prof")
        
        logger.info(f"📊 Starting cProfile snapshot for {duration_seconds}s...")
        
        pr = cProfile.Profile()
        pr.enable()
        
        # Wait for the specified duration
        time.sleep(duration_seconds)
        
        pr.disable()
        
        # Save the profile
        pr.dump_stats(profile_file)
        
        # Generate human-readable report
        with open(profile_file.replace('.prof', '.txt'), 'w') as f:
            stats = pstats.Stats(profile_file, stream=f)
            stats.sort_stats('cumulative')
            stats.print_stats(50)  # Top 50 functions
            
            f.write("\n" + "="*80 + "\n")
            f.write("TOP FUNCTIONS BY TOTAL TIME\n")
            f.write("="*80 + "\n")
            stats.sort_stats('tottime')
            stats.print_stats(50)
        
        logger.info(f"📊 cProfile snapshot saved: {profile_file}")
        logger.info(f"📊 Human-readable report: {profile_file.replace('.prof', '.txt')}")
        
        return profile_file
    
    async def start_continuous_monitoring(self):
        """Start continuous performance monitoring"""
        if not self.enabled:
            return
            
        logger.info(f"🔍 Starting continuous performance monitoring (interval: {self.profile_interval}s)")
        
        while True:
            try:
                await asyncio.sleep(self.profile_interval)
                
                # Print performance report
                self.print_performance_report()
                
                # Save snapshot if in detailed mode
                if self.profile_mode == 'detailed':
                    self.save_cprofile_snapshot(30)
                
                # Clear old data to prevent memory buildup
                with self.lock:
                    # Keep only recent data
                    for func_name in list(self.function_times.keys()):
                        times = self.function_times[func_name]
                        if len(times) > 1000:  # Keep last 1000 calls
                            self.function_times[func_name] = times[-1000:]
                            
            except asyncio.CancelledError:
                logger.info("🔍 Performance monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Error in performance monitoring: {e}")
                await asyncio.sleep(60)  # Wait before retrying

# Global profiler instance
profiler = LiveProfiler()

# Convenience functions
def profile_function(threshold_seconds=0.1):
    """Decorator to profile individual functions"""
    return profiler.profile_function(threshold_seconds)

def profile_block(block_name: str, threshold_seconds=0.1):
    """Context manager to profile code blocks"""
    return profiler.profile_block(block_name, threshold_seconds)

def get_performance_summary():
    """Get current performance statistics"""
    return profiler.get_performance_summary()

def print_performance_report():
    """Print a detailed performance report"""
    return profiler.print_performance_report()

def save_cprofile_snapshot(duration_seconds=30):
    """Save a detailed cProfile snapshot"""
    return profiler.save_cprofile_snapshot(duration_seconds)

async def start_continuous_monitoring():
    """Start continuous performance monitoring"""
    return await profiler.start_continuous_monitoring()

# Function call tracker for hotspot detection
class HotspotDetector:
    """
    Lightweight hotspot detector that tracks expensive operations
    """
    
    def __init__(self):
        self.enabled = os.getenv('ENABLE_HOTSPOT_DETECTION', 'true').lower() == 'true'
        self.threshold_seconds = float(os.getenv('HOTSPOT_THRESHOLD_SECONDS', '1.0'))
        self.hotspots = []
        self.lock = threading.Lock()
    
    def track_execution(self, function_name: str, execution_time: float, 
                       call_stack: Optional[str] = None):
        """Track function execution time and identify hotspots"""
        if not self.enabled or execution_time < self.threshold_seconds:
            return
            
        hotspot_info = {
            'function': function_name,
            'execution_time': execution_time,
            'timestamp': time.time(),
            'call_stack': call_stack or ''.join(traceback.format_stack()[-5:])
        }
        
        with self.lock:
            self.hotspots.append(hotspot_info)
            # Keep only recent hotspots
            if len(self.hotspots) > 100:
                self.hotspots = self.hotspots[-100:]
        
        logger.warning(f"🔥 HOTSPOT DETECTED: {function_name} took {execution_time:.3f}s")
    
    def get_recent_hotspots(self, minutes=10) -> List[Dict[str, Any]]:
        """Get hotspots from the last N minutes"""
        cutoff_time = time.time() - (minutes * 60)
        with self.lock:
            return [h for h in self.hotspots if h['timestamp'] > cutoff_time]
    
    def print_hotspot_summary(self):
        """Print summary of recent hotspots"""
        recent_hotspots = self.get_recent_hotspots(10)
        
        if not recent_hotspots:
            logger.info("🔥 No hotspots detected in the last 10 minutes")
            return
        
        logger.info(f"🔥 HOTSPOT SUMMARY (last 10 minutes): {len(recent_hotspots)} hotspots detected")
        
        # Group by function
        by_function = defaultdict(list)
        for hotspot in recent_hotspots:
            by_function[hotspot['function']].append(hotspot['execution_time'])
        
        # Show top problematic functions
        for func_name, times in sorted(by_function.items(), 
                                     key=lambda x: len(x[1]), reverse=True)[:5]:
            avg_time = sum(times) / len(times)
            max_time = max(times)
            logger.warning(f"  {len(times):2d}x {func_name}: avg {avg_time:.3f}s, max {max_time:.3f}s")

# Global hotspot detector
hotspot_detector = HotspotDetector()

def track_hotspot(function_name: str, execution_time: float, call_stack: Optional[str] = None):
    """Track a potential hotspot"""
    hotspot_detector.track_execution(function_name, execution_time, call_stack)

def get_recent_hotspots(minutes=10):
    """Get recent hotspots"""
    return hotspot_detector.get_recent_hotspots(minutes)

def print_hotspot_summary():
    """Print hotspot summary"""
    return hotspot_detector.print_hotspot_summary() 