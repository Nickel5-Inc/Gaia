"""
ABC Object Memory Leak Debugger
Provides detailed tracking of ABC object creation to identify memory leak sources
"""

import gc
import sys
import traceback
import weakref
import asyncio
from abc import ABC
from collections import defaultdict, Counter
from typing import Dict, List, Any, Set
import threading
import time
import logging

logger = logging.getLogger(__name__)

class ABCTracker:
    """
    Advanced ABC object tracker with detailed analytics
    """
    
    def __init__(self):
        self.tracked_objects: Set[weakref.ref] = set()
        self.creation_stats = defaultdict(int)
        self.type_stats = Counter()
        self.call_stack_stats = defaultdict(int)
        self.creation_times = {}
        self.lock = threading.Lock()
        self.start_time = time.time()
        
    def track_abc_creation(self, obj: ABC):
        """Track when an ABC object is created"""
        try:
            with self.lock:
                # Record type information (this doesn't require hashability)
                obj_type = type(obj).__name__
                module = type(obj).__module__
                full_type = f"{module}.{obj_type}"
                
                # Skip tracking substrate/scalecodec objects that cause issues
                if any(problematic in full_type.lower() for problematic in [
                    'metadataversioned', 'scalecodec', 'substrate', 'scale'
                ]):
                    # Still count the type but don't track the reference
                    self.type_stats[full_type] += 1
                    return
                
                # Try to create weak reference (not all objects support this)
                try:
                    obj_ref = weakref.ref(obj, self._cleanup_reference)
                    self.tracked_objects.add(obj_ref)
                except (TypeError, AttributeError):
                    # Object doesn't support weak references, just count it
                    pass
                
                self.type_stats[full_type] += 1
                self.creation_times[id(obj)] = time.time()
                
                # Capture call stack (limited to avoid performance issues)
                try:
                    stack = traceback.extract_stack()
                    # Get the most relevant frames (skip this function and __new__)
                    relevant_frames = []
                    for frame in stack[-10:-1]:  # Last 10 frames, excluding current
                        if 'abc_debugger.py' not in frame.filename:
                            relevant_frames.append(f"{frame.filename}:{frame.lineno}:{frame.name}")
                    
                    call_signature = " -> ".join(relevant_frames[-3:])  # Last 3 relevant frames
                    self.call_stack_stats[call_signature] += 1
                except Exception:
                    # Skip call stack tracking if it fails
                    pass
                    
        except Exception as e:
            # Gracefully handle any tracking errors to avoid breaking the main application
            logger.debug(f"ABC tracking error for {type(obj).__name__}: {e}")
            pass
    
    def _cleanup_reference(self, ref):
        """Cleanup when a tracked object is garbage collected"""
        with self.lock:
            self.tracked_objects.discard(ref)
    
    def get_current_stats(self) -> Dict[str, Any]:
        """Get current ABC object statistics"""
        with self.lock:
            # Clean up dead references
            dead_refs = {ref for ref in self.tracked_objects if ref() is None}
            self.tracked_objects -= dead_refs
            
            alive_count = len(self.tracked_objects)
            
            # Count current ABC objects in memory
            current_abc_objects = []
            for obj in gc.get_objects():
                if isinstance(obj, ABC):
                    current_abc_objects.append(obj)
            
            # Analyze current objects by type
            current_types = Counter()
            for obj in current_abc_objects:
                obj_type = type(obj).__name__
                module = type(obj).__module__
                full_type = f"{module}.{obj_type}"
                current_types[full_type] += 1
            
            return {
                'tracked_alive': alive_count,
                'total_in_memory': len(current_abc_objects),
                'creation_stats': dict(self.type_stats),
                'current_types': dict(current_types),
                'top_call_stacks': dict(Counter(self.call_stack_stats).most_common(10)),
                'runtime_seconds': time.time() - self.start_time
            }
    
    def print_detailed_report(self):
        """Print a detailed analysis of ABC object usage"""
        stats = self.get_current_stats()
        
        logger.info("=" * 60)
        logger.info("ABC OBJECT MEMORY LEAK ANALYSIS")
        logger.info("=" * 60)
        
        logger.info(f"Runtime: {stats['runtime_seconds']:.1f} seconds")
        logger.info(f"Current ABC objects in memory: {stats['total_in_memory']}")
        logger.info(f"Tracked references alive: {stats['tracked_alive']}")
        
        logger.info("\n--- TOP ABC OBJECT TYPES (Current in Memory) ---")
        for obj_type, count in sorted(stats['current_types'].items(), 
                                    key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"{count:6d} objects: {obj_type}")
        
        logger.info("\n--- ABC OBJECT CREATION HISTORY ---")
        for obj_type, count in sorted(stats['creation_stats'].items(), 
                                    key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"{count:6d} created: {obj_type}")
        
        logger.info("\n--- TOP CREATION CALL STACKS ---")
        for call_stack, count in sorted(stats['top_call_stacks'].items(), 
                                       key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"{count:4d} times: {call_stack}")
        
        logger.info("=" * 60)
        
        return stats

# Global tracker instance
_abc_tracker = ABCTracker()

def install_abc_tracker():
    """
    Install ABC creation tracking by monkey-patching ABC.__new__
    """
    original_new = ABC.__new__
    
    def tracked_new(cls, *args, **kwargs):
        instance = original_new(cls)
        _abc_tracker.track_abc_creation(instance)
        return instance
    
    ABC.__new__ = staticmethod(tracked_new)
    logger.info("ABC object tracking installed - monitoring all ABC object creation")

def get_abc_stats() -> Dict[str, Any]:
    """Get current ABC tracking statistics"""
    return _abc_tracker.get_current_stats()

def print_abc_report():
    """Print detailed ABC object analysis"""
    return _abc_tracker.print_detailed_report()

def find_abc_leaks(threshold: int = 1000) -> List[str]:
    """
    Find potential ABC memory leaks
    Returns list of problematic patterns
    """
    stats = get_abc_stats()
    issues = []
    
    if stats['total_in_memory'] > threshold:
        issues.append(f"HIGH ABC COUNT: {stats['total_in_memory']} objects in memory")
    
    # Find types with suspicious counts
    for obj_type, count in stats['current_types'].items():
        if count > 100:  # More than 100 instances of same type
            issues.append(f"HIGH TYPE COUNT: {count} instances of {obj_type}")
    
    # Find call stacks that create too many objects
    for call_stack, count in stats['top_call_stacks'].items():
        if count > 500:  # Called more than 500 times
            issues.append(f"HIGH CREATION FREQUENCY: {count} creations from {call_stack}")
    
    return issues

async def abc_leak_monitor(check_interval: int = 180):
    """
    Continuous ABC leak monitoring
    """
    logger.info(f"Starting ABC leak monitor (check every {check_interval}s)")
    
    while True:
        try:
            await asyncio.sleep(check_interval)
            
            leaks = find_abc_leaks()
            if leaks:
                logger.warning("ABC MEMORY LEAK DETECTED:")
                for leak in leaks:
                    logger.warning(f"  - {leak}")
                    
                # Print detailed report when leaks detected
                print_abc_report()
                
                # Force garbage collection
                collected = gc.collect()
                logger.info(f"Forced GC collected {collected} objects")
            
        except Exception as e:
            logger.error(f"Error in ABC leak monitor: {e}")
            await asyncio.sleep(60)  # Shorter retry interval 