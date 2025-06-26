"""
Performance Utilities - Drop-in replacements for common operations
Provides 2-10x performance improvements with graceful fallbacks.
"""

import sys
import logging
from typing import Any, Dict, Optional, Union, List, Tuple
import warnings

logger = logging.getLogger(__name__)

# ===== JSON UTILITIES (2-3x faster) =====
try:
    import orjson
    
    def dumps(obj: Any, **kwargs) -> str:
        """Ultra-fast JSON serialization using orjson (2-3x faster than json)."""
        try:
            # orjson returns bytes, convert to string for compatibility
            return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY).decode('utf-8')
        except TypeError:
            # Fallback for types orjson can't serialize
            import json
            def default_serializer(o):
                if hasattr(o, 'tolist'):  # numpy arrays
                    return o.tolist()
                if hasattr(o, 'item'):  # numpy scalars
                    return o.item()
                if hasattr(o, 'isoformat'):  # datetime objects
                    return o.isoformat()
                if isinstance(o, bytes):  # bytes objects
                    import base64
                    return {"_type": "bytes", "data": base64.b64encode(o).decode('ascii')}
                raise TypeError(f"Object of type {type(o)} is not JSON serializable")
            return json.dumps(obj, default=default_serializer, **kwargs)
    
    def loads(s: Union[str, bytes]) -> Any:
        """Ultra-fast JSON deserialization using orjson (2-3x faster than json)."""
        return orjson.loads(s)
    
    def dumps_bytes(obj: Any) -> bytes:
        """Ultra-fast JSON serialization returning bytes directly."""
        try:
            return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)
        except TypeError:
            # Fallback for types orjson can't serialize
            return dumps(obj).encode('utf-8')
    
    JSON_LIBRARY = "orjson"
    logger.info("🚀 Using orjson for 2-3x faster JSON processing")
    
except ImportError:
    import json
    
    def dumps(obj: Any, **kwargs) -> str:
        """Fallback JSON serialization using standard library."""
        # Handle numpy types in fallback
        def default_serializer(o):
            if hasattr(o, 'tolist'):  # numpy arrays
                return o.tolist()
            if hasattr(o, 'item'):  # numpy scalars
                return o.item()
            if hasattr(o, 'isoformat'):  # datetime objects
                return o.isoformat()
            raise TypeError(f"Object of type {type(o)} is not JSON serializable")
        
        return json.dumps(obj, default=default_serializer, **kwargs)
    
    def loads(s: Union[str, bytes]) -> Any:
        """Fallback JSON deserialization using standard library."""
        return json.loads(s)
    
    def dumps_bytes(obj: Any) -> bytes:
        """Fallback JSON serialization returning bytes."""
        return dumps(obj).encode('utf-8')
    
    JSON_LIBRARY = "json (standard)"
    logger.info("⚡ Using standard json library - install orjson for 2-3x performance boost")


# ===== SERIALIZATION UTILITIES (5-10x faster) =====
try:
    import msgpack
    
    def pack(obj: Any, **kwargs) -> bytes:
        """Ultra-fast binary serialization using msgpack (5-10x faster than pickle)."""
        # Handle datetime objects by converting to ISO strings
        def default_serializer(o):
            if hasattr(o, 'isoformat'):  # datetime objects
                return o.isoformat()
            raise TypeError(f"Object of type {type(o)} is not msgpack serializable")
        
        return msgpack.packb(obj, use_bin_type=True, default=default_serializer, **kwargs)
    
    def unpack(data: bytes, **kwargs) -> Any:
        """Ultra-fast binary deserialization using msgpack."""
        return msgpack.unpackb(data, raw=False, **kwargs)
    
    SERIALIZATION_LIBRARY = "msgpack"
    logger.info("🚀 Using msgpack for 5-10x faster binary serialization")
    
except ImportError:
    import pickle
    
    def pack(obj: Any, **kwargs) -> bytes:
        """Fallback binary serialization using pickle."""
        return pickle.dumps(obj, **kwargs)
    
    def unpack(data: bytes, **kwargs) -> Any:
        """Fallback binary deserialization using pickle."""
        return pickle.loads(data, **kwargs)
    
    SERIALIZATION_LIBRARY = "pickle (standard)"
    logger.info("⚡ Using standard pickle - install msgpack for 5-10x performance boost")


# ===== COMPRESSION UTILITIES (faster compression) =====
try:
    import lz4.frame
    
    def compress(data: bytes, **kwargs) -> bytes:
        """Fast compression using lz4 (excellent speed/ratio for large data)."""
        return lz4.frame.compress(data, **kwargs)
    
    def decompress(data: bytes, **kwargs) -> bytes:
        """Fast decompression using lz4."""
        return lz4.frame.decompress(data, **kwargs)
    
    COMPRESSION_LIBRARY = "lz4"
    logger.info("🚀 Using lz4 for fast compression with good ratio")
    
except ImportError:
    import gzip
    
    def compress(data: bytes, **kwargs) -> bytes:
        """Fallback compression using gzip."""
        return gzip.compress(data, **kwargs)
    
    def decompress(data: bytes, **kwargs) -> bytes:
        """Fallback decompression using gzip."""
        return gzip.decompress(data, **kwargs)
    
    COMPRESSION_LIBRARY = "gzip (standard)"
    logger.info("⚡ Using standard gzip - install lz4 for faster compression")


# ===== CHARACTER ENCODING UTILITIES (faster encoding detection) =====
try:
    import cchardet
    
    def detect_encoding(data: bytes) -> Optional[Dict[str, Any]]:
        """Fast character encoding detection using cchardet (C-based)."""
        return cchardet.detect(data)
    
    ENCODING_LIBRARY = "cchardet"
    logger.info("🚀 Using cchardet for fast character encoding detection")
    
except ImportError:
    try:
        import chardet
        
        def detect_encoding(data: bytes) -> Optional[Dict[str, Any]]:
            """Fallback character encoding detection using chardet."""
            return chardet.detect(data)
        
        ENCODING_LIBRARY = "chardet (standard)"
        logger.info("⚡ Using standard chardet - install cchardet for faster encoding detection")
        
    except ImportError:
        def detect_encoding(data: bytes) -> Optional[Dict[str, Any]]:
            """No encoding detection available."""
            return {'encoding': 'utf-8', 'confidence': 0.5}
        
        ENCODING_LIBRARY = "none (fallback)"
        logger.warning("No encoding detection library available")


# ===== PERFORMANCE MONITORING =====
def get_performance_info() -> Dict[str, str]:
    """Get information about which performance libraries are active."""
    return {
        "json": JSON_LIBRARY,
        "serialization": SERIALIZATION_LIBRARY, 
        "compression": COMPRESSION_LIBRARY,
        "encoding": ENCODING_LIBRARY,
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
    }


def log_performance_status():
    """Log the current performance optimization status."""
    info = get_performance_info()
    logger.info("🔧 Performance Libraries Status:")
    for category, library in info.items():
        if category != "python_version":
            emoji = "🚀" if "standard" not in library and "fallback" not in library else "⚡"
            logger.info(f"  {emoji} {category.capitalize()}: {library}")
    logger.info(f"  🐍 Python: {info['python_version']}")


# ===== OPTIMIZED UTILITIES FOR SPECIFIC USE CASES =====

def serialize_miner_payload(payload: Dict[str, Any]) -> bytes:
    """Optimized serialization for miner communication payloads."""
    try:
        # Use msgpack for internal data, JSON for compatibility
        if 'data' in payload and isinstance(payload['data'], dict):
            # Compress large data sections
            data_json = dumps_bytes(payload['data'])
            if len(data_json) > 1024:  # > 1KB, use compression
                payload['data'] = compress(data_json)
                payload['_compressed'] = True
        
        return dumps_bytes(payload)
    except Exception as e:
        logger.warning(f"Optimized serialization failed, using fallback: {e}")
        return dumps_bytes(payload)


def deserialize_miner_response(response_bytes: bytes) -> Any:
    """Optimized deserialization for miner responses."""
    try:
        response = loads(response_bytes)
        
        # Handle compressed data
        if isinstance(response, dict) and response.get('_compressed'):
            if 'data' in response:
                response['data'] = loads(decompress(response['data']))
                del response['_compressed']
        
        return response
    except Exception as e:
        logger.warning(f"Optimized deserialization failed, using fallback: {e}")
        return loads(response_bytes)


# ===== BATCH PROCESSING OPTIMIZATIONS =====

def process_large_dataset_batch(data_list: List[Any], chunk_size: int = 1000) -> List[bytes]:
    """Process large datasets in optimized chunks."""
    results = []
    for i in range(0, len(data_list), chunk_size):
        chunk = data_list[i:i + chunk_size]
        # Use fastest available serialization + compression
        serialized = pack(chunk)
        if len(serialized) > 10240:  # > 10KB
            serialized = compress(serialized)
        results.append(serialized)
    return results


# Initialize performance monitoring
if __name__ != "__main__":
    # Only log when imported, not when run directly
    log_performance_status() 