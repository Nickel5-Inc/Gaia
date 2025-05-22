import base64
import json
import pickle
import logging
from typing import Any, Dict, Optional

# Conditional import for Batch type hint
try:
    from aurora import Batch
    _AURORA_AVAILABLE = True
except ImportError:
    Batch = Any # type: ignore
    _AURORA_AVAILABLE = False
    logging.warning("Aurora SDK not found. Batch type is Any. Functionality might be limited if Batch objects are expected.")

logger = logging.getLogger(__name__)

async def prepare_input_batch_from_payload(payload_str: str, config: Dict[str, Any]) -> Optional[Batch]:
    """
    Deserializes a base64 encoded, pickled Aurora Batch object from a JSON payload string.
    The payload_str is expected to be a JSON string containing a "serialized_aurora_batch" field.
    """
    try:
        payload_dict = json.loads(payload_str)
        if "serialized_aurora_batch" not in payload_dict:
            logger.error("'serialized_aurora_batch' field missing in the JSON payload.")
            return None

        serialized_batch_b64 = payload_dict["serialized_aurora_batch"]
        if not isinstance(serialized_batch_b64, str):
            logger.error("'serialized_aurora_batch' must be a base64 encoded string.")
            return None

        logger.debug("Decoding base64 serialized batch...")
        pickled_batch_bytes = base64.b64decode(serialized_batch_b64)

        logger.debug("Unpickling batch data...")
        deserialized_batch = pickle.loads(pickled_batch_bytes)

        if _AURORA_AVAILABLE:
            if not isinstance(deserialized_batch, Batch): # type: ignore
                logger.error(f"Deserialized object is not an Aurora Batch. Type: {type(deserialized_batch)}")
                return None
        elif deserialized_batch is None: # Basic check if Aurora not available
            logger.error("Deserialized batch is None.")
            return None

        logger.info("Successfully deserialized input Aurora Batch.")
        return deserialized_batch

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in payload: {e}")
        return None
    except base64.binascii.Error as e:
        logger.error(f"Base64 decoding error: {e}")
        return None
    except pickle.UnpicklingError as e:
        logger.error(f"Unpickling error: {e}")
        return None
    except TypeError as e: # Catches potential errors if data isn't bytes for b64decode or pickle.loads
        logger.error(f"Type error during deserialization: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in prepare_input_batch_from_payload: {e}", exc_info=True)
        return None

async def serialize_prediction_step(
    prediction_step: Batch,
    step_index: int,
    config: Dict[str, Any]
) -> Optional[str]:
    """
    Serializes a single prediction step (Aurora Batch object) into a JSON string.
    The batch is pickled, then base64 encoded.
    Includes metadata like step index and forecast hours.
    """
    if _AURORA_AVAILABLE and not isinstance(prediction_step, Batch): # type: ignore
        logger.error(f"Object to serialize is not an Aurora Batch. Type: {type(prediction_step)}. Skipping step {step_index}.")
        return None
    elif prediction_step is None: # Basic check if Aurora not available
        logger.error(f"Prediction_step is None for step_index {step_index}. Skipping.")
        return None

    try:
        logger.debug(f"Pickling prediction step {step_index}...")
        pickled_prediction = pickle.dumps(prediction_step)

        logger.debug(f"Base64 encoding pickled prediction step {step_index}...")
        serialized_data_b64 = base64.b64encode(pickled_prediction).decode('utf-8')

        model_config = config.get('model', {})
        data_proc_config = config.get('data_processing', {})

        output_dict = {
            "step_index": step_index,
            "forecast_step_hours": (step_index + 1) * model_config.get('forecast_step_hours', 6),
            "serialized_prediction": serialized_data_b64,
            "format": data_proc_config.get('serialization_format', 'pickle_base64')
        }

        logger.info(f"Successfully serialized prediction step {step_index}.")
        return json.dumps(output_dict)

    except pickle.PicklingError as e:
        logger.error(f"Pickling error for step {step_index}: {e}")
        return None
    except TypeError as e: # Catches errors like trying to b64encode non-bytes
        logger.error(f"Type error during serialization for step {step_index}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in serialize_prediction_step for step {step_index}: {e}", exc_info=True)
        return None 