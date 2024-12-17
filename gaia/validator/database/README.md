# Database and Cache Setup

This directory contains the database and cache configuration for the validator. It uses PostgreSQL for the main database and Redis for caching.

## Prerequisites

- PostgreSQL 12+
- Redis 6+
- Python 3.8+

## Installation

1. Install required packages:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
cp .env.example .env
```
Edit `.env` with your configuration values.

## Redis Setup

1. Install Redis:
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install redis-server

# macOS
brew install redis

# Start Redis service
sudo systemctl start redis  # Linux
brew services start redis   # macOS
```

2. Configure Redis:
```bash
# Verify Redis is running
redis-cli ping  # Should return PONG

# Set password (recommended for production)
redis-cli
> CONFIG SET requirepass "your_redis_password"
> CONFIG REWRITE
> exit
```

3. Initialize Redis for the validator:
```bash
python -m gaia.validator.database.setup_redis
```

## Database Configuration

1. Create database and user:
```sql
CREATE DATABASE validator_db;
CREATE USER validator WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE validator_db TO validator;
```

2. Run database migrations:
```bash
python -m gaia.validator.database.migrations
```

## Verification

1. Check database connection:
```python
from gaia.validator.database.database_manager import DatabaseManager

async def verify():
    db = DatabaseManager()
    status = await db.health_check()
    print(f"Database status: {status}")
```

2. Check Redis connection:
```python
from gaia.validator.database.setup_redis import verify_redis_connection

async def verify():
    status = await verify_redis_connection()
    print(f"Redis status: {status}")
```

## Configuration Options

### Redis Configuration

- `REDIS_HOST`: Redis server host (default: localhost)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_PASSWORD`: Redis password (required in production)
- `REDIS_MAX_CONNECTIONS`: Maximum number of connections (default: 10)
- `REDIS_HEALTH_CHECK_INTERVAL`: Health check interval in seconds (default: 30)

### Cache TTL Settings

- `CACHE_TTL_DEFAULT`: Default TTL for cached items (300s)
- `CACHE_TTL_MINER_INFO`: TTL for miner information (300s)
- `CACHE_TTL_RECENT_SCORES`: TTL for recent scores (60s)
- `CACHE_TTL_ACTIVE_MINERS`: TTL for active miners list (60s)

### Database Connection Pool

- `DB_POOL_SIZE`: Initial pool size (default: 20)
- `DB_MAX_OVERFLOW`: Maximum number of connections above pool size (default: 10)
- `DB_POOL_TIMEOUT`: Connection timeout in seconds (default: 30)
- `DB_POOL_RECYCLE`: Connection recycle time in seconds (default: 1800)

## Monitoring

The system provides several monitoring endpoints:

1. Database health:
```python
health_status = await db_manager.health_check()
```

2. Redis status:
```python
redis_status = await cache_manager.get_stats()
```

3. Connection pool status:
```python
pool_status = await db_manager.get_pool_status()
```

## Troubleshooting

1. Redis connection issues:
   - Check if Redis is running: `redis-cli ping`
   - Verify password: `redis-cli -a your_password ping`
   - Check logs: `sudo journalctl -u redis`

2. Database connection issues:
   - Check PostgreSQL status: `sudo systemctl status postgresql`
   - Verify connection: `psql -U validator -d validator_db`
   - Check logs: `sudo tail -f /var/log/postgresql/postgresql.log`

## Best Practices

1. Always use environment variables for sensitive configuration
2. Monitor cache hit rates and adjust TTLs accordingly
3. Regularly check connection pool metrics
4. Implement proper error handling for both database and cache operations
5. Use health checks in production monitoring 