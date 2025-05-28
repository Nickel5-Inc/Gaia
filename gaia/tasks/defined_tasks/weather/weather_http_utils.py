import pickle
import base64
from typing import Optional, List, Callable
import xarray as xr
from aurora import Batch # Assuming Batch is importable here, or adjust as needed
import logging # For logger instance
import gzip
import fsspec
import uuid
import io # Added for BytesIO
import tarfile
from datetime import datetime # For base_time typing, though not used in this function directly

# It's good practice to have a module-level logger if these utils become complex
# For now, we'll expect a logger instance to be passed if needed by specific functions.

def serialize_aurora_batch_for_service(batch: Batch) -> bytes:
    """Serializes an aurora.Batch object for sending to an inference service."""
    # The logger for this function would typically be passed from the calling context (e.g., WeatherTask's logger)
    # For simplicity in this standalone function, direct logging calls are omitted unless a logger is passed.
    try:
        pickled_batch = pickle.dumps(batch)
        # The original method in WeatherTask did b64encode(pickled_batch).
        # The _run_inference_via_http_service then did .encode('utf-8') on this.
        # For clarity, let's ensure this function returns the final bytes ready for HTTP.
        # If the service expects raw bytes (base64 encoded bytes), this is correct.
        # If it expects a base64 *string*, then it should be .decode('utf-8') here,
        # and the HTTP client would handle encoding.
        # Given the previous pattern:
        # serialized_batch_bytes = self._serialize_aurora_batch_for_service(initial_batch)
        # client.post(upload_url, content=serialized_batch_bytes, headers=headers)
        # This implies _serialize_aurora_batch_for_service should return bytes.
        return base64.b64encode(pickled_batch)
    except Exception as e:
        # Consider how to log here. If a logger is passed: logger.error(f"Error serializing aurora.Batch: {e}", exc_info=True)
        # For now, re-raise to let the caller handle it, as this util function doesn't have its own logger context.
        raise # Re-raise the exception to be caught by the caller 

def deserialize_prediction_from_service(response_data: bytes, logger_instance: logging.Logger) -> Optional[xr.Dataset]:
    """
    Deserializes gzipped netCDF data from an inference service response into an xr.Dataset.
    Args:
        response_data: Raw bytes from the HTTP response.
        logger_instance: Logger to use for logging errors/info.
    Returns:
        An xr.Dataset or None if deserialization fails.
    """
    try:
        # Attempt to decompress assuming it's gzipped NetCDF data
        uncompressed_data = gzip.decompress(response_data)
        # Load dataset from bytes in memory using fsspec and a BytesIO buffer for seeking
        # Using a unique name for memory file, though not strictly necessary as it's in-memory
        with io.BytesIO(uncompressed_data) as buffer:
            buffer.seek(0)
            ds = xr.open_dataset(buffer, engine="h5netcdf") # or engine="netcdf4" if appropriate and available
        logger_instance.info(f"Successfully deserialized gzipped prediction step. Dataset summary:\n{ds}")
        return ds
    except gzip.BadGzipFile:
        logger_instance.error("Failed to decompress: Bad Gzip File. Response data might not be gzipped.", exc_info=True)
        # Attempt to load directly if not gzipped (or if error in gzip assumption)
        try:
            with io.BytesIO(response_data) as buffer:
                buffer.seek(0)
                ds = xr.open_dataset(buffer, engine="h5netcdf") # or engine="netcdf4"
            logger_instance.info("Successfully deserialized non-gzipped data after gzip error.")
            return ds
        except Exception as e_direct:
            logger_instance.error(f"Failed to deserialize directly after gzip error: {e_direct}", exc_info=True)
            return None
    except Exception as e:
        logger_instance.error(f"Error deserializing prediction from service: {e}", exc_info=True)
        return None 

def extract_and_deserialize_tar_archives(
    archive_bytes_list: List[bytes], 
    # base_time: datetime, # base_time is not directly used here, but kept if deserialize_func needs it
    deserialize_func: Callable[[bytes, logging.Logger], Optional[xr.Dataset]],
    logger_instance: logging.Logger
) -> List[xr.Dataset]:
    """
    Extracts gzipped NetCDF files from a list of tar.gz archives (in bytes),
    deserializes each file into an xr.Dataset using the provided deserialize_func.

    Args:
        archive_bytes_list: A list where each element is the byte content of a .tar.gz archive.
        deserialize_func: The function to call for deserializing each extracted step file's bytes.
                          Expected signature: (file_bytes: bytes, logger: logging.Logger) -> Optional[xr.Dataset]
        logger_instance: Logger to use for logging errors/info.

    Returns:
        A list of xr.Dataset objects, intended to be in correct forecast step order (relies on tar member order).
    """
    all_datasets: List[xr.Dataset] = []    
    for i, archive_bytes in enumerate(archive_bytes_list):
        logger_instance.info(f"Processing archive {i+1}/{len(archive_bytes_list)} (Size: {len(archive_bytes)} bytes)...")
        try:
            with io.BytesIO(archive_bytes) as archive_fileobj:
                with tarfile.open(fileobj=archive_fileobj, mode="r:gz") as tar: # Open as gzipped tar
                    # Get members and sort them by name to ensure order (e.g., step_0.nc.gz, step_1.nc.gz)
                    # This relies on consistent naming of files within the archive.
                    members = sorted(tar.getmembers(), key=lambda m: m.name)
                    
                    if not members:
                        logger_instance.warning(f"Archive {i+1} is empty or contains no processable members.")
                        continue

                    for member in members:
                        # Process only files, and expect .nc or .nc.gz (handled by deserialize_func)
                        if member.isfile() and (member.name.endswith(".nc") or member.name.endswith(".nc.gz")):
                            logger_instance.debug(f"Extracting file '{member.name}' from archive {i+1}...")
                            extracted_member_bytes = None
                            try:
                                extracted_fileobj = tar.extractfile(member)
                                if extracted_fileobj:
                                    extracted_member_bytes = extracted_fileobj.read()
                                else:
                                    logger_instance.warning(f"Could not get file-like object for member '{member.name}' from archive {i+1}.")
                                    continue 
                            except Exception as e_extract_member:
                                logger_instance.error(f"Error extracting member '{member.name}' from archive {i+1}: {e_extract_member}", exc_info=True)
                                continue # Skip this member

                            if extracted_member_bytes:
                                try:
                                    # The deserialize_func (e.g., deserialize_prediction_from_service)
                                    # is responsible for handling whether the bytes are gzipped or not.
                                    dataset_step = deserialize_func(extracted_member_bytes, logger_instance)
                                    
                                    if dataset_step is not None:
                                        # Ensure 'lead_time' coordinate exists after deserialization for concat
                                        # This check is important for later concatenation.
                                        if 'lead_time' not in dataset_step.coords and 'lead_time' not in dataset_step.dims:
                                            logger_instance.warning(f"Deserialized step '{member.name}' from archive {i+1} is missing 'lead_time' coordinate. Concatenation might fail or be incorrect.")
                                        
                                        all_datasets.append(dataset_step)
                                        logger_instance.debug(f"Successfully deserialized '{member.name}' from archive {i+1}.")
                                    else:
                                        logger_instance.warning(f"Deserialization of '{member.name}' from archive {i+1} returned None.")
                                except Exception as e_deser:
                                    logger_instance.error(f"Error deserializing step from extracted file '{member.name}' (archive {i+1}): {e_deser}", exc_info=True)
                            else:
                                logger_instance.warning(f"Extracted zero bytes for '{member.name}' from archive {i+1}.")
                        elif member.isfile():
                            logger_instance.debug(f"Skipping non-NetCDF file '{member.name}' in archive {i+1}.")
        
        except tarfile.ReadError as e_tar_read:
            logger_instance.error(f"Error reading tar archive {i+1} (maybe corrupted or not a tar.gz file?): {e_tar_read}", exc_info=True)
        except Exception as e_archive_proc: # Catch any other errors during this archive's processing
            logger_instance.error(f"Generic error processing archive {i+1}: {e_archive_proc}", exc_info=True)
            
    if not all_datasets:
        logger_instance.warning("No datasets were successfully extracted and deserialized from any archives.")
    else:
        logger_instance.info(f"Successfully extracted and deserialized {len(all_datasets)} datasets from all archives.")
        
    return all_datasets 