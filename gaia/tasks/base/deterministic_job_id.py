import hashlib
import uuid
from datetime import datetime, timezone
from typing import Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class DeterministicJobID:
    """
    Generates deterministic job IDs that are consistent across validator replicas.
    
    CRITICAL: Uses SCHEDULED/PREDETERMINED timestamps (not current time) to ensure
    perfect consistency across all nodes regardless of processing timing differences.
    
    Examples of scheduled timestamps:
    - Weather: GFS initialization time (predetermined by weather model schedule)
    - Geomagnetic: Target prediction time (predetermined by hourly schedule)  
    - Soil Moisture: SMAP target time (predetermined by satellite schedule)
    
    This eliminates variations from network latency, processing delays, or clock drift.
    """
    
    @staticmethod
    def generate_weather_job_id(
        gfs_init_time: datetime,
        miner_hotkey: str,
        validator_hotkey: str,
        job_type: str = "forecast"
    ) -> str:
        """
        Generate deterministic job ID for weather tasks.
        
        IMPORTANT: Uses the predetermined GFS initialization time, NOT current time.
        This ensures all validator nodes generate identical job IDs regardless of
        when they process the same forecast request.
        
        Args:
            gfs_init_time: GFS initialization time (SCHEDULED, not processing time)
            miner_hotkey: Miner's hotkey
            validator_hotkey: Validator's hotkey  
            job_type: Type of job (forecast, fetch, etc.)
            
        Returns:
            Deterministic UUID string
        """
        # Normalize timestamp to ensure consistency across all nodes
        gfs_init_time = DeterministicJobID.normalize_scheduled_time(gfs_init_time)
        
        # Create seed components
        timestamp_str = gfs_init_time.strftime("%Y%m%d%H%M%S")
        seed_components = [
            "weather_job",
            job_type,
            timestamp_str,
            miner_hotkey,
            validator_hotkey
        ]
        
        # Generate deterministic UUID
        seed_string = "_".join(seed_components)
        return DeterministicJobID._generate_uuid_from_seed(seed_string)
    
    @staticmethod
    def generate_geomagnetic_job_id(
        query_time: datetime,
        miner_hotkey: str,
        validator_hotkey: str
    ) -> str:
        """
        Generate deterministic job ID for geomagnetic tasks.
        
        IMPORTANT: Uses the predetermined target prediction time, NOT current time.
        Typically this is the scheduled hourly prediction time (e.g., 14:00:00 UTC).
        
        Args:
            query_time: Target prediction time (SCHEDULED, not processing time)
            miner_hotkey: Miner's hotkey
            validator_hotkey: Validator's hotkey
            
        Returns:
            Deterministic UUID string
        """
        # Normalize timestamp to ensure consistency across all nodes
        query_time = DeterministicJobID.normalize_scheduled_time(query_time)
        
        timestamp_str = query_time.strftime("%Y%m%d%H%M%S")
        seed_components = [
            "geomagnetic_job",
            timestamp_str,
            miner_hotkey,
            validator_hotkey
        ]
        
        seed_string = "_".join(seed_components)
        return DeterministicJobID._generate_uuid_from_seed(seed_string)
    
    @staticmethod
    def generate_soil_moisture_job_id(
        target_time: datetime,
        miner_hotkey: str,
        validator_hotkey: str,
        region_bbox: Optional[str] = None
    ) -> str:
        """
        Generate deterministic job ID for soil moisture tasks.
        
        IMPORTANT: Uses the predetermined SMAP target time, NOT current time.
        This is the scheduled satellite data collection time that all nodes
        are working toward, ensuring identical job IDs.
        
        Args:
            target_time: Target SMAP time (SCHEDULED, not processing time)
            miner_hotkey: Miner's hotkey
            validator_hotkey: Validator's hotkey
            region_bbox: Optional bbox string for spatial tasks
            
        Returns:
            Deterministic UUID string
        """
        # Normalize timestamp to ensure consistency across all nodes
        target_time = DeterministicJobID.normalize_scheduled_time(target_time)
        
        timestamp_str = target_time.strftime("%Y%m%d%H%M%S")
        seed_components = [
            "soil_moisture_job",
            timestamp_str,
            miner_hotkey,
            validator_hotkey
        ]
        
        if region_bbox:
            seed_components.append(region_bbox)
        
        seed_string = "_".join(seed_components)
        return DeterministicJobID._generate_uuid_from_seed(seed_string)
    
    @staticmethod
    def generate_task_id(
        task_name: str,
        target_time: datetime,
        additional_context: Optional[str] = None
    ) -> str:
        """
        Generate deterministic task ID for scoring tasks.
        
        Args:
            task_name: Name of the task
            target_time: Target time for the task
            additional_context: Optional additional context
            
        Returns:
            Deterministic task ID string
        """
        # Normalize timestamp to ensure consistency across all nodes
        target_time = DeterministicJobID.normalize_scheduled_time(target_time)
        
        timestamp_str = target_time.strftime("%Y%m%d%H%M%S")
        seed_components = [task_name, "task", timestamp_str]
        
        if additional_context:
            seed_components.append(additional_context)
        
        seed_string = "_".join(seed_components)
        # For task IDs, use a shorter hash instead of full UUID
        return hashlib.sha256(seed_string.encode()).hexdigest()[:16]
    
    @staticmethod
    def _generate_uuid_from_seed(seed_string: str) -> str:
        """
        Generate a deterministic UUID from a seed string.
        
        Args:
            seed_string: String to use as seed
            
        Returns:
            UUID string in standard format
        """
        # Create SHA256 hash of seed
        hash_bytes = hashlib.sha256(seed_string.encode()).digest()
        
        # Use first 16 bytes to create UUID
        uuid_bytes = hash_bytes[:16]
        
        # Set version (4) and variant bits to make it a valid UUID4
        uuid_bytes = bytearray(uuid_bytes)
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x40  # Version 4
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80  # Variant bits
        
        # Convert to UUID object and return as string
        deterministic_uuid = uuid.UUID(bytes=bytes(uuid_bytes))
        
        logger.debug(f"Generated deterministic UUID: {deterministic_uuid} from seed: {seed_string[:50]}...")
        return str(deterministic_uuid)
    
    @staticmethod
    def normalize_scheduled_time(
        scheduled_time: datetime,
        round_to_seconds: bool = True
    ) -> datetime:
        """
        Normalize a scheduled timestamp to ensure consistency across nodes.
        
        This removes microseconds and optionally rounds to ensure all nodes
        work with exactly the same timestamp values.
        
        Args:
            scheduled_time: The scheduled time to normalize
            round_to_seconds: Whether to round to nearest second (default: True)
            
        Returns:
            Normalized datetime with timezone info
        """
        # Ensure UTC timezone
        if scheduled_time.tzinfo is None:
            scheduled_time = scheduled_time.replace(tzinfo=timezone.utc)
        elif scheduled_time.tzinfo != timezone.utc:
            scheduled_time = scheduled_time.astimezone(timezone.utc)
        
        # Remove microseconds for consistency
        normalized = scheduled_time.replace(microsecond=0)
        
        if round_to_seconds:
            # Round to nearest second to handle any sub-second variations
            if normalized.microsecond >= 500000:
                normalized = normalized.replace(second=normalized.second + 1, microsecond=0)
        
        logger.debug(f"Normalized scheduled time: {normalized}")
        return normalized
    
    @staticmethod
    def is_deterministic_job_id(job_id: str) -> bool:
        """
        Check if a job ID was generated deterministically.
        This is a heuristic based on the fact that deterministic UUIDs
        will have consistent patterns when generated from similar seeds.
        
        Args:
            job_id: Job ID to check
            
        Returns:
            True if likely deterministic, False otherwise
        """
        try:
            # Check if it's a valid UUID format
            uuid.UUID(job_id)
            return True
        except ValueError:
            # If it's not a UUID, it might be a task ID (hash-based)
            return len(job_id) == 16 and all(c in '0123456789abcdef' for c in job_id.lower()) 