import asyncio
import anyio
from gaia.validator.validator import GaiaValidator
from gaia.tasks.defined_tasks.soilmoisture.validator.soil_validator_task import SoilValidatorTask
from gaia.tasks.defined_tasks.geomagnetic.validator.geomagnetic_validator_task import GeomagneticValidatorTask
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.flows import ValidatorFlows
from prefect import serve
from prefect.client.schemas.schedules import IntervalSchedule
from argparse import ArgumentParser
from datetime import timedelta

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
    
    args = parser.parse_args([])  # Parse empty list to get default values
    return args

async def deploy_flows():
    """Deploy all flows with schedules."""
    db_manager = ValidatorDatabaseManager()
    args = create_validator_args()
    validator = GaiaValidator(args=args)
    
    soil_task = SoilValidatorTask(
        db_manager=db_manager,
        node_type="validator",
        test_mode=False
    )
    
    geomagnetic_task = GeomagneticValidatorTask(
        node_type="validator",
        db_manager=db_manager,
        test_mode=False
    )
    
    flows = ValidatorFlows()
    
    core_schedule = IntervalSchedule(interval=timedelta(minutes=5))
    scoring_schedule = IntervalSchedule(interval=timedelta(minutes=10))
    task_schedule = IntervalSchedule(interval=timedelta(minutes=15))
    monitoring_schedule = IntervalSchedule(interval=timedelta(minutes=5))
    soil_schedule = IntervalSchedule(interval=timedelta(minutes=5))
    geomagnetic_schedule = IntervalSchedule(interval=timedelta(hours=1))
    
    core_flow = await flows.core_flow(validator)
    await core_flow.deploy(
        name="validator-core",
        schedules=[core_schedule],
        work_pool_name="default"
    )
    
    scoring_flow = await flows.scoring_flow(validator)
    await scoring_flow.deploy(
        name="validator-scoring",
        schedules=[scoring_schedule],
        work_pool_name="default"
    )
    
    task_flow = await flows.task_processing_flow(validator)
    await task_flow.deploy(
        name="validator-tasks",
        schedules=[task_schedule],
        work_pool_name="default"
    )
    
    monitoring_flow = await flows.monitoring_flow(validator)
    await monitoring_flow.deploy(
        name="validator-monitoring",
        schedules=[monitoring_schedule],
        work_pool_name="default"
    )
    
    soil_flow = await soil_task.validator_execute(validator)
    await soil_flow.deploy(
        name="soil-validator",
        schedules=[soil_schedule],
        work_pool_name="default"
    )
    
    geomagnetic_flow = await geomagnetic_task.execute_flow(validator)
    await geomagnetic_flow.deploy(
        name="geomagnetic-validator",
        schedules=[geomagnetic_schedule],
        work_pool_name="default"
    )

if __name__ == "__main__":
    asyncio.run(deploy_flows()) 