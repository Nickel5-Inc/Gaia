# Gaia Database Migration Guide

This guide explains how to use the separate Alembic configurations for miner and validator databases to ensure schema convergence across all nodes.

## Overview

The system now uses **two separate Alembic configurations**:
- `alembic_miner.ini` - For miner database schema
- `alembic_validator.ini` - For validator database schema

This approach ensures that:
1. Each database type has the exact schema it needs
2. No cross-contamination between miner and validator tables
3. All nodes converge to identical schemas regardless of their current state
4. Schema validation prevents missing essential tables

## Migration Files

### Miner Database
- **Configuration**: `alembic_miner.ini`
- **Migration Directory**: `alembic_migrations_miner/`
- **Target Table**: `weather_miner_jobs` with complete column set
- **Target Database**: `miner_db`

### Validator Database  
- **Configuration**: `alembic_validator.ini`
- **Migration Directory**: `alembic_migrations_validator/`
- **Core Tables**: `node_table`, `score_table`, `weather_forecast_runs`, `weather_miner_responses`, plus optional tables
- **Target Database**: `validator_db`

## Usage Commands

### For Miner Nodes
```bash
# Check current schema version
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini current

# View migration history
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini history

# Upgrade to latest schema (convergence migration)
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini upgrade head

# View revision details
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini show head
```

### For Validator Nodes
```bash
# Check current schema version
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini current

# View migration history  
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini history

# Upgrade to latest schema (validation migration)
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini upgrade head

# View revision details
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini show head
```

## How the Convergence Migrations Work

### Miner Migration (`15952c9da69b`)
The miner convergence migration:

1. **Validates Environment**: Inspects current database state
2. **Removes Forbidden Tables**: Drops any validator-specific tables that shouldn't exist
3. **Ensures Core Table**: Creates or validates `weather_miner_jobs` table
4. **Column Management**: Adds missing columns, removes unexpected ones
5. **Index Management**: Creates required indexes, removes unexpected ones

**Key Features**:
- Handles divergent database states gracefully
- Adds missing columns with appropriate defaults for non-nullable fields
- Preserves existing data while ensuring schema compliance
- Provides detailed logging of all changes

### Validator Migration (`2e00df6800b9`)
The validator validation migration:

1. **Core Table Validation**: Ensures essential tables exist (`node_table`, `score_table`, etc.)
2. **Column Validation**: Verifies required columns are present
3. **Forbidden Table Removal**: Drops miner-specific tables if present
4. **Detailed Reporting**: Provides comprehensive schema report
5. **Graceful Failure**: Fails with clear error messages if essential tables are missing

**Key Features**:
- Validates minimum required schema for validator operations
- Does not attempt to recreate missing core tables (prevents data loss)
- Provides clear error messages for manual resolution
- Reports optional missing tables as warnings

## Troubleshooting

### Common Issues

#### 1. "Missing core tables" Error (Validator)
```
ERROR: Missing core tables: {'node_table', 'score_table'}
```
**Solution**: This indicates the validator database is missing essential tables. You need to either:
- Restore from a backup with proper schema
- Run the initial schema migration first
- Manually create the missing tables

#### 2. Connection Issues
```
FAILED: Can't connect to database
```
**Solution**: Check your database connection settings:
- Verify PostgreSQL is running
- Check `DB_CONNECTION_TYPE` environment variable
- Verify database exists and permissions are correct

#### 3. "Target database is not up to date" Error
```
FAILED: Target database is not up to date.
```
**Solution**: This usually means you need to upgrade first:
```bash
DB_CONNECTION_TYPE=socket alembic -c alembic_[miner|validator].ini upgrade head
```

### Database State Verification

#### Check Miner Database Schema
```bash
DB_CONNECTION_TYPE=socket psql postgresql://postgres:postgres@/miner_db -c "\dt"
DB_CONNECTION_TYPE=socket psql postgresql://postgres:postgres@/miner_db -c "\d weather_miner_jobs"
```

#### Check Validator Database Schema  
```bash
DB_CONNECTION_TYPE=socket psql postgresql://postgres:postgres@/validator_db -c "\dt"
DB_CONNECTION_TYPE=socket psql postgresql://postgres:postgres@/validator_db -c "\d node_table"
```

## Environment Variables

The migrations use these environment variables:

- `DB_CONNECTION_TYPE`: Set to `socket` for Unix socket connections
- `DB_HOST`: Database host (when not using socket)
- `DB_PORT`: Database port
- `DB_USER`: Database username  
- `DB_PASSWORD`: Database password

## Migration Safety

### Data Preservation
- **Existing Data**: All migrations preserve existing data
- **Column Additions**: New columns use appropriate defaults
- **Table Drops**: Only drops forbidden tables (cross-contamination prevention)

### Rollback Strategy
- The convergence migrations do not implement rollback (`downgrade()`)
- Always backup your database before running migrations
- Test migrations on a copy of production data first

### Validation
- Miner migration validates the final schema matches expected structure
- Validator migration validates minimum required tables and columns exist
- Both provide detailed logging of all changes made

## Best Practices

1. **Always Backup First**: Create database backups before running migrations
2. **Test on Staging**: Run migrations on staging/test environments first
3. **Monitor Logs**: Review migration output carefully for any warnings
4. **Verify Results**: Check database schema after migration completion
5. **Coordinate Deployments**: Ensure all nodes run the same migration version

## Advanced Usage

### Custom Database URLs
You can override the database URL by setting environment variables:
```bash
export DB_HOST=your-host
export DB_PORT=5432
export DB_USER=your-user
export DB_PASSWORD=your-password
```

### Manual Migration Creation
To create new migrations (for developers):
```bash
# For miner
DB_CONNECTION_TYPE=socket alembic -c alembic_miner.ini revision -m "description"

# For validator  
DB_CONNECTION_TYPE=socket alembic -c alembic_validator.ini revision -m "description"
```

This guide ensures all nodes can achieve identical database schemas regardless of their starting state, enabling reliable operation across the entire network. 