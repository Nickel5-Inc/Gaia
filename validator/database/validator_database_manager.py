import asyncpg
from database.database_manager import BaseDatabaseManager
from typing import Optional, List, Dict, Any
import os
import json
from pathlib import Path

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
        """
        Initialize the validator database schema.
        Creates all necessary tables for validator operations.


        Process Queue:
            The process queue is a table that stores the status of processes related to tasks. 
            The validator will loop through the queue and execute tasks based on their status/priority.
            For example - tasks will have preprocessing, querying, and postprocessing steps.

        """
        # Get Task-Specific Table Schemas
        await self.load_task_schemas()
        async with self.get_connection() as conn:
            # Create core validator tables
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS process_queue (
                    name TEXT,
                    uuid TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    priority INTEGER DEFAULT 0
                      
                );
            """)
    



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

