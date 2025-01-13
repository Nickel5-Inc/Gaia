# Validator Performance Tracking

## Current Known Issues
1. Network operations without proper timeouts
2. Database operations without proper timeouts
3. Infinite loops without proper break conditions
4. Large data processing bottlenecks
5. Resource cleanup issues
6. Synchronization issues
7. External API dependency issues
8. Memory management issues

## Improvement Plan

### Phase 1: Monitoring & Detection
- [x] Add detailed operation timing logs
- [x] Implement timeout tracking for key operations
- [x] Add memory usage monitoring
- [x] Track resource usage per task

### Phase 2: Critical Fixes
- [x] Implement proper timeouts for network operations
- [x] Add circuit breakers for external APIs
- [x] Fix resource cleanup issues
- [x] Improve database operation timeouts

### Phase 3: Optimization
- [x] Implement chunking for large data operations
- [ ] Optimize memory usage
- [ ] Improve task synchronization
- [ ] Add proper error recovery mechanisms

## Progress Log

### Resource Cleanup Implementation  
- Added comprehensive resource cleanup mechanism:
  - Temporary file cleanup (*.h5, *.tif, *.tiff, *.tmp)
  - Task-specific resource cleanup for soil and geomagnetic tasks
  - Database cleanup (hanging operations, connection pools)
  - Task state cleanup and reset
  - Memory cleanup with forced garbage collection
  - HTTP client cleanup and reset
- Integrated cleanup with task recovery and error handling:
  - Automatic cleanup on task failures
  - Cleanup before task restarts
  - Cleanup on main loop errors
  - Final cleanup on application exit

### Resource Tracking Implementation 
- Enhanced task health tracking with comprehensive resource monitoring:
  - Per-task memory usage tracking (start, current, peak)
  - CPU usage monitoring
  - Open file handles tracking
  - Thread count monitoring
  - Automatic alerts for significant memory increases (>50%)
- Added detailed resource logging:
  - Operation start/end resource snapshots
  - Memory change tracking between operations
  - Peak memory usage alerts
  - Resource usage trends during watchdog checks

### Database Operation Improvements
- Enhanced database operations with comprehensive timeout handling:
  - Added default timeouts for different operations:
    - Query timeout: 30 seconds
    - Transaction timeout: 180 seconds (3 minutes)
    - Connection timeout: 10 seconds
  - Implemented connection pool health monitoring
  - Added automatic connection pool reset after 3 consecutive failures
  - Enhanced error handling and logging for database operations
  - Added timeout decorator for all database operations

### Network Operation Improvements
- Enhanced query_miners with proper timeout handling:
  - Added 30-second timeout for handshakes
  - Added 3-minute timeout for miner queries
  - Implemented circuit breaker pattern (stops after 5 consecutive failures)
  - Added better success/failure tracking
  - Improved error logging and reporting

### Enhanced Monitoring
- Enhanced watchdog with detailed timing logs
- Added memory usage tracking during timeouts
- Improved timeout detection and logging
- Added tracking for slow database operations and metagraph syncs
- Updated psutil requirement for memory monitoring

###  Initial Setup
- Created tracking document
- Identified key areas for improvement

### Thread-Based Task Management
- Implemented comprehensive thread metrics tracking:
  - Per-task execution metrics:
    - Start/completion/error counts
    - Runtime tracking (current, total, average)
    - Lock wait times
    - Cleanup operations
    - Memory usage (start, peak, changes)
  - Thread identification and state tracking
  - Resource usage monitoring
  - Lock contention tracking
- Enhanced logging for thread operations:
  - Periodic metric summaries
  - Memory usage alerts
  - Lock contention warnings
  - Task state transitions

## Implementation Notes
Each improvement should be tested thoroughly before moving to production. Document any issues or unexpected behavior here.

### Database Operation Implementation Notes
- Timeouts:
  - Query operations: 30 seconds
  - Transactions: 180 seconds
  - Connection establishment: 10 seconds
- Connection Pool Management:
  - Pool size: 5 connections
  - Max overflow: 10 connections
  - Pre-ping enabled for connection health checks
  - Auto-reset after 3 consecutive failures
- Look for log entries:
  - "Database operation timed out" for timeout events
  - "Multiple consecutive database failures" for connection issues
  - "Resetting database connection pool" for recovery attempts

### Network Operation Implementation Notes
- Timeouts:
  - Handshake: 30 seconds
  - Miner Query: 180 seconds (3 minutes)
- Circuit Breaker:
  - Triggers after 5 consecutive failures
  - Helps prevent cascading failures
  - Resets counter on successful operations
- Look for log entries:
  - "Circuit breaker triggered" for system protection events
  - "Successfully queried X miners" for operation summaries
  - "Timeout for miner" for specific timeout instances

### Monitoring Implementation Notes
- The enhanced watchdog now logs:
  - Operation duration for each task
  - Memory usage when timeouts occur
  - Detailed status of frozen tasks
  - Slow database operations (>5s)
  - Slow metagraph syncs (>30s)
- Look for log entries starting with:
  - "TIMEOUT_ALERT" for operation timeouts
  - "FREEZE_DETECTED" for frozen tasks
  - "Memory Usage at Timeout" for memory stats 

### Resource Tracking Implementation Notes
- Resource metrics tracked:
  - Memory usage (RSS): start, current, and peak
  - CPU percentage
  - Open file handles count
  - Thread count
- Monitoring points:
  - Operation start: Initial snapshot
  - Operation end: Final snapshot with change metrics
  - Watchdog checks: Regular updates during task execution
- Alert thresholds:
  - Memory increase > 50% of peak
  - Significant thread count changes
  - Unusual file handle counts
- Log entries to watch for:
  - "Task {name} started operation" - includes initial memory usage
  - "Task {name} completed operation" - includes memory change and peak
  - "High memory usage in task {name}" - indicates potential memory issues 

### Resource Cleanup Implementation Notes
- Cleanup triggers:
  - Task failure or completion
  - Main loop errors
  - Application shutdown
  - Manual recovery requests
- Cleanup operations:
  - Temporary files: Removes data files from /tmp
  - Database: Resets hanging operations to 'pending'
  - Memory: Forces garbage collection
  - HTTP: Closes and recreates client with fresh settings
  - Task states: Resets to idle with cleared metrics
- Log entries to watch for:
  - "Starting comprehensive resource cleanup" - cleanup initiation
  - "Cleaned up temp file" - successful file removal
  - "Reset hanging database operations" - database cleanup
  - "Reset database connection pool" - connection cleanup
  - "Forced garbage collection" - memory cleanup
  - "Reset HTTP client" - client cleanup
  - "Completed resource cleanup" - successful completion 

### Implementation Notes

#### Thread Metrics Details
- Per-Task Metrics:
  - Execution counts (starts, completions, errors)
  - Timing metrics (last start, completion, error)
  - Runtime statistics (total, average, current)
  - Memory tracking (peak usage, significant changes)
  - Thread identification
  - Lock wait time accumulation
  - Cleanup operation counting

#### Thread Monitoring Points
- Metric Collection:
  - Task start: Initial state and memory capture
  - Task completion: Runtime calculation and stats update
  - Error occurrence: Error counting and timing
  - Lock acquisition: Wait time tracking
  - Cleanup operations: Resource management tracking
  - Memory usage: Continuous monitoring with alerts

#### Thread Management Alerts
- Memory Usage:
  - Alerts for >50% increase from start
  - Peak memory tracking per task
  - Memory change logging on completion
- Lock Contention:
  - Wait time tracking per task
  - Contention pattern identification
- Error Patterns:
  - Error frequency tracking
  - Error timing analysis
  - Cleanup effectiveness monitoring 