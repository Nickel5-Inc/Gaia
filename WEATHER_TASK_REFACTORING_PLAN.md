# Weather Task Refactoring Plan

## Current State Analysis
- **`weather_task.py`**: 5,738 lines - Massive monolith with mixed responsibilities
- **`weather_scoring_mechanism.py`**: 1,838 lines - Complex scoring logic
- **Mixed concerns**: Configuration, HTTP, R2 storage, worker management, miner/validator logic

## Refactoring Strategy

### Phase 1: Core Task Interface
**File**: `gaia/tasks/defined_tasks/weather/core/task.py` (~150 lines)
- Clean WeatherTask class with minimal interface
- Constructor and basic configuration loading
- Delegate all specific functionality to specialized modules

### Phase 2: Configuration Management  
**File**: `gaia/tasks/defined_tasks/weather/core/config.py` (~200 lines)
- Extract `_load_config()` function
- Centralized environment variable handling
- Type-safe configuration validation

### Phase 3: Validator Components
**Directory**: `gaia/tasks/defined_tasks/weather/validator/`

**3A. Validator Core** - `validator/core.py` (~300 lines)
- `validator_execute()` main loop
- `validator_prepare_subtasks()`
- Run lifecycle management

**3B. Miner Communication** - `validator/miner_client.py` (~400 lines)
- Miner polling and triggering logic
- HTTP communication with miners
- Response verification and tracking

**3C. Scoring Orchestration** - `validator/scoring.py` (~300 lines)
- Scoring job management
- Initial and final scoring triggers
- Score aggregation and database updates

### Phase 4: Miner Components
**Directory**: `gaia/tasks/defined_tasks/weather/miner/`

**4A. Miner Core** - `miner/core.py` (~250 lines)
- `miner_execute()` and preprocessing
- Job lifecycle management
- Basic miner functionality

**4B. HTTP Handlers** - `miner/http_handlers.py` (~400 lines)
- `handle_initiate_fetch()`
- `handle_get_input_status()`
- `handle_start_inference()`
- `handle_kerchunk_request()`

**4C. Inference Management** - `miner/inference.py` (~300 lines)
- Local model inference
- HTTP service integration
- Azure Foundry integration
- RunPod integration

### Phase 5: Storage & I/O
**Directory**: `gaia/tasks/defined_tasks/weather/storage/`

**5A. R2 Storage** - `storage/r2_client.py` (~200 lines)
- R2 upload/download operations
- S3 client management
- Credential handling

**5B. File Management** - `storage/file_manager.py` (~150 lines)
- Local file operations
- Cache management
- Cleanup operations

### Phase 6: Worker Management
**Directory**: `gaia/tasks/defined_tasks/weather/workers/`

**6A. Worker Manager** - `workers/manager.py` (~200 lines)
- Background worker lifecycle
- Worker starting/stopping
- Resource management

**6B. Worker Implementations** - Use existing `processing/weather_workers.py`
- Already well-structured
- Minor imports adjustments needed

### Phase 7: Scoring Refactoring
**Directory**: `gaia/tasks/defined_tasks/weather/scoring/`

**7A. Day-1 Scoring** - `scoring/day1.py` (~600 lines)
- Extract `evaluate_miner_forecast_day1()`
- Parallel processing optimizations
- Variable processing logic

**7B. ERA5 Final Scoring** - `scoring/era5.py` (~400 lines)
- ERA5 scoring logic
- Climatology handling
- Final score computation

**7C. Scoring Utilities** - `scoring/utils.py` (~300 lines)
- Shared scoring functions
- Metric calculations
- Data preprocessing

**7D. Parallel Processing** - `scoring/parallel.py` (~400 lines)
- `_process_single_timestep_parallel()`
- `_process_single_variable_parallel()`
- Async processing optimizations

### Phase 8: Utilities & Common
**Directory**: `gaia/tasks/defined_tasks/weather/common/`

**8A. Progress Tracking** - `common/progress.py` (~100 lines)
- Progress callbacks and updates
- Status tracking
- File location management

**8B. Era5 Climatology** - `common/climatology.py` (~200 lines)
- ERA5 climatology loading
- Caching mechanisms
- Codec registration

## Implementation Order

1. ✅ **Core Task Interface** - Clean entry point
2. ✅ **Configuration Management** - Centralized config
3. ✅ **Validator Components** - Largest functionality area
4. ✅ **Miner Components** - Second largest area  
5. ✅ **Storage & I/O** - Cross-cutting infrastructure
6. ✅ **Worker Management** - Background processing
7. ✅ **Scoring Refactoring** - Complex scoring logic
8. ✅ **Utilities & Common** - Shared functionality

## Expected Benefits
- **Maintainability**: 5,738 lines → ~3,200 lines across 16 focused modules
- **Testing**: Each module can be unit tested independently
- **Performance**: Better module loading and memory management
- **Team Development**: Multiple developers can work on different areas
- **Code Quality**: Single Responsibility Principle applied throughout

## Migration Strategy
- Keep original files during transition
- Update imports progressively  
- Maintain backward compatibility
- Test each module independently
- Complete integration testing before removing originals