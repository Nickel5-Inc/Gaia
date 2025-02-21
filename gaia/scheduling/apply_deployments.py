import asyncio
from argparse import ArgumentParser
from datetime import timedelta
from prefect import get_client, flow
import logging
from pathlib import Path
import os

from gaia.scheduling.config import get_schedule_config
from gaia.validator.flows import core_flow, scoring_flow, task_processing_flow, monitoring_flow, ValidatorFlows

logger = logging.getLogger(__name__)

def create_validator_args():
    """Create arguments for GaiaValidator."""
    parser = ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Run in test mode')
    parser.add_argument('--netuid', type=int, default=1, help='Network UID')
    parser.add_argument('--subtensor.network', type=str, default='finney', help='Subtensor network')
    parser.add_argument('--logging.debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--neuron.name', type=str, default='validator', help='Neuron name')
    parser.add_argument('--wallet.name', type=str, default='validator', help='Wallet name')
    parser.add_argument('--wallet.hotkey', type=str, default='default', help='Wallet hotkey')
    
    args = parser.parse_args([])
    return args


def get_schedule_kwargs(config_name: str):
    """Retrieve schedule parameters from config and return kwargs for deploy."""
    config = get_schedule_config(config_name)
    kwargs = {}
    if config.cron:
        kwargs['cron'] = config.cron
    elif config.interval:
        kwargs['interval'] = int(config.interval.total_seconds())
    return kwargs


def get_project_root() -> Path:
    """Get the project root directory by finding the git root or walking up until we find gaia/."""
    current_path = Path(__file__).resolve()
    
    for parent in current_path.parents:
        if (parent / '.git').exists():
            return parent
            
    for parent in current_path.parents:
        if (parent / 'gaia').is_dir():
            return parent
            
    logger.warning("Could not find project root, using current working directory")
    return Path.cwd()


async def deploy_flows():
    """Deploy all flows using from_source() and deploy() with config-based scheduling parameters."""
    from gaia.validator.validator import GaiaValidator
    from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
    from gaia.tasks.defined_tasks.soilmoisture.validator.soil_flows import soil_validator_workflow
    from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_flows import geomagnetic_validator_workflow
    
    db_manager = ValidatorDatabaseManager()
    args = create_validator_args()
    validator = GaiaValidator(args=args)
    
    base_path = get_project_root()
    logger.info(f"Using project root path: {base_path}")
    
    core_kwargs = get_schedule_kwargs("validator_core_flow")
    scoring_kwargs = get_schedule_kwargs("scoring_flow")
    task_kwargs = get_schedule_kwargs("task_processing_flow")
    monitoring_kwargs = get_schedule_kwargs("monitoring_flow")
    soil_kwargs = get_schedule_kwargs("soil_moisture_task")
    geomagnetic_kwargs = get_schedule_kwargs("geomagnetic_task")
    
    # Directories for each flow
    validator_flows_dir = str(base_path)
    soil_flows_dir = str(base_path)
    geomagnetic_flows_dir = str(base_path)
    
    # Deploy the validator flows 
    deployment = await (await ValidatorFlows.core_flow.from_source(
        source=validator_flows_dir,
        entrypoint="gaia/validator/flows.py:core_flow"
    )).deploy(
        name="validator-core",
        work_pool_name="default",
        **core_kwargs
    )
    
    deployment = await (await ValidatorFlows.scoring_flow.from_source(
        source=validator_flows_dir,
        entrypoint="gaia/validator/flows.py:scoring_flow"
    )).deploy(
        name="validator-scoring",
        work_pool_name="default",
        **scoring_kwargs
    )
    
    deployment = await (await ValidatorFlows.task_processing_flow.from_source(
        source=validator_flows_dir,
        entrypoint="gaia/validator/flows.py:task_processing_flow"
    )).deploy(
        name="validator-tasks",
        work_pool_name="default",
        **task_kwargs
    )
    
    deployment = await (await ValidatorFlows.monitoring_flow.from_source(
        source=validator_flows_dir,
        entrypoint="gaia/validator/flows.py:monitoring_flow"
    )).deploy(
        name="validator-monitoring",
        work_pool_name="default",
        **monitoring_kwargs
    )
    
    # Deploy standalone flows (defined tasks)
    deployment = await (await soil_validator_workflow.from_source(
        source=soil_flows_dir,
        entrypoint="gaia/tasks/defined_tasks/soilmoisture/validator/soil_flows.py:soil_validator_workflow"
    )).deploy(
        name="soil-validator",
        work_pool_name="default",
        **soil_kwargs
    )
    
    deployment = await (await geomagnetic_validator_workflow.from_source(
        source=geomagnetic_flows_dir,
        entrypoint="gaia/tasks/defined_tasks/geomagnetic/validator/geomagnetic_flows.py:geomagnetic_validator_workflow"
    )).deploy(
        name="geomagnetic-validator",
        work_pool_name="default",
        **geomagnetic_kwargs
    )


if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    asyncio.run(deploy_flows())