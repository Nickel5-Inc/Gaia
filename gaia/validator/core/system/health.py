"""
System Health Monitoring
========================

Functions for checking system health including database, network, and resource status.
"""

import asyncio
from typing import Dict, Any, Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


async def check_system_health(
    database: Any = None,
    network: Any = None,
    memory_status: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Comprehensive system health check.
    
    Args:
        database: Database manager instance
        network: Network components
        memory_status: Current memory status
        
    Returns:
        Dict containing health status for all components
    """
    health_status = {
        'overall': 'healthy',
        'database': {'status': 'unknown'},
        'network': {'status': 'unknown'},
        'memory': {'status': 'unknown'},
        'issues': []
    }
    
    # Database health check
    if database:
        try:
            # Simple connectivity test
            await database.execute("SELECT 1")
            health_status['database'] = {'status': 'healthy'}
        except Exception as e:
            health_status['database'] = {'status': 'unhealthy', 'error': str(e)}
            health_status['issues'].append(f"Database unhealthy: {e}")
    
    # Network health check
    if network:
        try:
            # Check if we have substrate connection
            if hasattr(network, 'substrate') and network.substrate:
                health_status['network'] = {'status': 'healthy'}
            else:
                health_status['network'] = {'status': 'unhealthy', 'error': 'No substrate connection'}
                health_status['issues'].append("Network unhealthy: No substrate connection")
        except Exception as e:
            health_status['network'] = {'status': 'unhealthy', 'error': str(e)}
            health_status['issues'].append(f"Network unhealthy: {e}")
    
    # Memory health check
    if memory_status:
        status = memory_status.get('threshold_status', 'unknown')
        if status in ['critical', 'emergency']:
            health_status['memory'] = {'status': 'unhealthy', 'level': status}
            health_status['issues'].append(f"Memory {status}: {memory_status.get('memory_mb', 0):.1f} MB")
        elif status == 'warning':
            health_status['memory'] = {'status': 'degraded', 'level': status}
        else:
            health_status['memory'] = {'status': 'healthy'}
    
    # Determine overall health
    if health_status['issues']:
        health_status['overall'] = 'unhealthy'
    elif any(comp.get('status') == 'degraded' for comp in 
             [health_status['database'], health_status['network'], health_status['memory']]):
        health_status['overall'] = 'degraded'
    
    return health_status 