import asyncpg
from database.database_manager import BaseDatabaseManager
from typing import Optional, List, Dict, Any
import os
import json
from pathlib import Path
from datetime import datetime

class ValidatorDatabaseManager(BaseDatabaseManager):
    """
    Database manager specifically for validator nodes.
    Handles all validator-specific database operations.
    """
    
    def __init__(self, host: str = 'localhost', 
                 port: int = 5432, user: str = 'postgres', 
                 password: str = 'postgres', min_size: int = 5, 
                 max_size: int = 20):
        """
        Initialize the validator database manager.
        
        Args:
            host (str, optional): Database host. Defaults to 'localhost'
            port (int, optional): Database port. Defaults to 5432
            user (str, optional): Database user. Defaults to 'postgres'
            password (str, optional): Database password. Defaults to 'postgres'
            min_size (int, optional): Minimum pool size. Defaults to 5
            max_size (int, optional): Maximum pool size. Defaults to 20
        """
        super().__init__('validator', host, port, user, password, min_size, max_size)

    async def initialize_database(self):
        """Initialize the queue tables and task-specific tables"""

        async with self.get_connection() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS process_queue (
                    id SERIAL PRIMARY KEY,
                    process_type VARCHAR(50) NOT NULL,  -- 'network' or 'compute'
                    process_name VARCHAR(100) NOT NULL,
                    task_id INTEGER, -- id of the calling task
                    task_name VARCHAR(100), -- name of the calling task
                    priority INTEGER DEFAULT 0, 
                    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed 
                    payload BYTEA,
                    start_processing_time TIMESTAMP WITH TIME ZONE, -- move task from pending to processing (for scheduled tasks that can't execute immediately)
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP WITH TIME ZONE,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    complete_by TIMESTAMP WITH TIME ZONE, -- Some tasks may have a deadline - we should increase priority as deadline approaches.
                    expected_execution_time INTEGER, -- Expected time taken to complete the task on the minimum hardware. 
                    execution_time INTEGER, -- Time taken to complete the task
                    error TEXT,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3
                );

                CREATE INDEX IF NOT EXISTS idx_process_queue_status ON process_queue(status);
                CREATE INDEX IF NOT EXISTS idx_process_queue_priority ON process_queue(priority);
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS task_queue (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    task_type VARCHAR(50) NOT NULL,
                    task_name VARCHAR(100) NOT NULL,
                    process_ids (INTEGER,TEXT), -- list of processes ids that are spawned for this task
                    
                    complete_by TIMESTAMP WITH TIME ZONE, -- deadline for task completion
                    payload BYTEA,
                    
                );
            """)
        
        task_schemas = await self.load_task_schemas()
        await self.initialize_task_tables(task_schemas)

    ##### QUEUE TABLE FUNCTIONS #####
    async def add_to_queue(self, 
                          process_type: str, 
                          process_name: str, 
                          payload: bytes,
                          task_id: Optional[int] = None,
                          task_name: Optional[str] = None,
                          priority: int = 0,
                          complete_by: Optional[datetime] = None,
                          expected_execution_time: Optional[int] = None):
        """
        Add a process to the queue.
        
        Args:
            process_type (str): Type of process ('network' or 'compute')
            process_name (str): Name of the process
            payload (bytes): Binary data payload for the process
            task_id (Optional[int]): ID of the calling task if applicable
            task_name (Optional[str]): Name of the calling task
            priority (int): Priority level (higher = more priority)
            complete_by (Optional[datetime]): Deadline for task completion
            expected_execution_time (Optional[int]): Expected execution time in seconds
        """
        async with self.get_connection() as conn:
            await conn.execute("""
                INSERT INTO process_queue (
                    process_type,
                    process_name,
                    payload,
                    task_id,
                    task_name,
                    priority,
                    complete_by,
                    expected_execution_time
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, 
            process_type,
            process_name,
            payload,
            task_id,
            task_name,
            priority,
            complete_by,
            expected_execution_time
            )

    async def get_next_task(self, task_type: str = None):
        """Get the next task from the queue"""
        async with self.get_connection() as conn:
            query = """
                UPDATE process_queue 
                SET status = 'processing', started_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM process_queue
                    WHERE status = 'pending'
                    AND retries < max_retries
                    {}
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING *
            """.format("AND task_type = $1" if task_type else "")
            
            return await conn.fetchrow(query, task_type if task_type else None)

    async def complete_task(self, task_id: int, error: str = None):
        """Mark a task as completed or failed"""
        async with self.get_connection() as conn:
            if error:
                await conn.execute("""
                    UPDATE process_queue 
                    SET status = 'failed',
                        completed_at = CURRENT_TIMESTAMP,
                        error = $2,
                        retries = retries + 1
                    WHERE id = $1
                """, task_id, error)
            else:
                await conn.execute("""
                    UPDATE process_queue 
                    SET status = 'completed',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, task_id)



    ##### LOAD TASK SPECIFIC TABLES #####
    async def load_task_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        Load database schemas for all tasks from their respective schema.json files.
        Searches through the defined_tasks directory for schema definitions.

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping task names to their schema definitions
            
        Example schema.json format:
        {
            "table_name": "task_validation",
            "columns": {
                "id": "UUID PRIMARY KEY",
                "status": "TEXT NOT NULL",
                "data": "JSONB",
                "created_at": "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
            },
            "indexes": [
                {"column": "status", "unique": false},
                {"column": "created_at", "unique": false}
            ]
        }
        """
        # Get the absolute path to the defined_tasks directory
        try:
            # First try to find it in the package
            import tasks
            tasks_dir = Path(tasks.__file__).parent / 'defined_tasks'
            
            if not tasks_dir.exists():
                # Fallback to relative path from current file
                base_dir = Path(__file__).parent.parent.parent
                tasks_dir = base_dir / 'tasks' / 'defined_tasks'
            
            if not tasks_dir.exists():
                raise FileNotFoundError(f"Tasks directory not found at {tasks_dir}")
                
        except ImportError:
            # If tasks package is not installed, use relative path
            base_dir = Path(__file__).parent.parent.parent
            tasks_dir = base_dir / 'tasks' / 'defined_tasks'
            
            if not tasks_dir.exists():
                raise FileNotFoundError(f"Tasks directory not found at {tasks_dir}")

        schemas = {}
        
        # Loop through all subdirectories in the tasks directory
        for task_dir in tasks_dir.iterdir():
            if task_dir.is_dir():
                schema_file = task_dir / 'schema.json'
                
                # Skip if no schema file exists
                if not schema_file.exists():
                    continue
                
                try:
                    # Load and validate the schema
                    with open(schema_file, 'r') as f:
                        schema = json.load(f)
                        
                    # Validate required schema components
                    if not all(key in schema for key in ['table_name', 'columns']):
                        raise ValueError(
                            f"Invalid schema in {schema_file}. "
                            "Must contain 'table_name' and 'columns'"
                        )
                    
                    # Create the table for this task
                    await self._create_task_table(schema)
                    
                    # Store the validated schema
                    schemas[task_dir.name] = schema
                    
                except json.JSONDecodeError as e:
                    print(f"Error parsing schema.json in {task_dir.name}: {e}")
                except Exception as e:
                    print(f"Error processing schema for {task_dir.name}: {e}")
                    
        return schemas

    async def _create_task_table(self, schema: Dict[str, Any]):
        """
        Create a database table for a task based on its schema definition.
        
        Args:
            schema (Dict[str, Any]): The schema definition for the task
        """
        # Build the CREATE TABLE query
        columns = [f"{col_name} {col_type}" 
                  for col_name, col_type in schema['columns'].items()]
        
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema['table_name']} (
                {','.join(columns)}
            )
        """
        
        async with self.get_connection() as conn:
            async with conn.transaction():
                # Create the table
                await conn.execute(create_table_query)
                
                # Create any specified indexes
                if 'indexes' in schema:
                    for index in schema['indexes']:
                        await self.create_index(
                            schema['table_name'],
                            index['column'],
                            unique=index.get('unique', False)
                        )

    async def initialize_task_tables(self, task_schemas: Dict[str, Dict[str, Any]]):
        """Initialize validator-specific task tables."""
        for schema in task_schemas.values():
            if schema.get('database_type') in ['validator', 'both']:
                await self._create_task_table(schema)
