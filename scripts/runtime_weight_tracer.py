#!/usr/bin/env python3
"""
Runtime Weight Tracer

This script patches fiber library functions at runtime (in memory) 
without modifying files, avoiding syntax errors and file corruption.

Usage: Import this module in your validator before weight setting operations.
"""

import sys
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
import traceback

# Add fiber path
sys.path.insert(0, '/root/.gaia/lib/python3.10/site-packages')

class RuntimeWeightTracer:
    def __init__(self, log_file: str = "/root/Gaia/weight_trace_runtime.log"):
        self.log_file = log_file
        self.original_functions = {}
        self.is_patched = False
        self.trace_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize log file
        with open(self.log_file, 'w') as f:
            f.write(f"=== Runtime Weight Trace Started: {self.trace_id} ===\n\n")
    
    def log_weights(self, context: str, weights: Any, metadata: Optional[Dict] = None):
        """Log weight information safely"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            # Format weights safely
            if weights is None:
                weights_summary = "None"
            elif hasattr(weights, 'tolist'):  # PyTorch tensor
                try:
                    weights_list = weights.tolist()
                    non_zero = [w for w in weights_list if w > 0]
                    weights_summary = f"Total: {len(weights_list)}, NonZero: {len(non_zero)}, Max: {max(non_zero) if non_zero else 0:.6f}, Sample: {weights_list[:5]}"
                except:
                    weights_summary = f"Tensor type: {type(weights)}, Shape: {getattr(weights, 'shape', 'unknown')}"
            elif hasattr(weights, '__len__'):  # List/array
                try:
                    non_zero = [w for w in weights if w > 0]
                    weights_summary = f"Total: {len(weights)}, NonZero: {len(non_zero)}, Max: {max(non_zero) if non_zero else 0:.6f}, Sample: {list(weights)[:5]}"
                except:
                    weights_summary = f"List type: {type(weights)}, Length: {len(weights)}"
            else:
                weights_summary = f"Type: {type(weights).__name__}, Value: {str(weights)[:50]}"
            
            log_entry = f"[WEIGHT_TRACE] {timestamp} - {context}\n"
            log_entry += f"Weights: {weights_summary}\n"
            if metadata:
                try:
                    log_entry += f"Metadata: {json.dumps(metadata, default=str)}\n"
                except:
                    log_entry += f"Metadata: {str(metadata)}\n"
            log_entry += "-" * 80 + "\n"
            
            # Write to both stdout and file
            print(f"[WEIGHT_TRACE] {context}: {weights_summary}")
            
            try:
                with open(self.log_file, "a") as f:
                    f.write(log_entry)
            except Exception as file_error:
                print(f"[WEIGHT_TRACE_FILE_ERROR] {file_error}")
                
        except Exception as e:
            print(f"[WEIGHT_TRACE_ERROR] {e}")
    
    def patch_fiber_functions(self):
        """Patch fiber functions at runtime"""
        try:
            print("🔍 [RUNTIME_TRACER] Importing fiber library...")
            import fiber.chain.weights as fiber_weights
            
            print(f"✅ [RUNTIME_TRACER] Fiber library loaded from: {fiber_weights.__file__}")
            
            # Store original functions
            self.original_functions['set_node_weights'] = fiber_weights.set_node_weights
            self.original_functions['normalize_and_quantize'] = fiber_weights._normalize_and_quantize_weights
            self.original_functions['send_weights_to_chain'] = fiber_weights._send_weights_to_chain
            
            # Create traced version of set_node_weights
            def traced_set_node_weights(substrate, keypair, node_ids, node_weights, netuid, validator_node_id, **kwargs):
                tracer.log_weights("FIBER_ENTRY", node_weights, {
                    "function": "set_node_weights",
                    "netuid": netuid,
                    "validator_uid": validator_node_id,
                    "node_count": len(node_ids),
                    "version_key": kwargs.get('version_key', 0),
                    "wait_for_inclusion": kwargs.get('wait_for_inclusion', False),
                    "wait_for_finalization": kwargs.get('wait_for_finalization', False)
                })
                
                # Call original function
                result = tracer.original_functions['set_node_weights'](
                    substrate, keypair, node_ids, node_weights, netuid, validator_node_id, **kwargs
                )
                
                tracer.log_weights("FIBER_EXIT", node_weights, {
                    "function": "set_node_weights",
                    "result": result,
                    "success": result
                })
                
                return result
            
            # Create traced version of _normalize_and_quantize_weights
            def traced_normalize_and_quantize(node_ids, node_weights):
                tracer.log_weights("NORMALIZATION_INPUT", node_weights, {
                    "function": "_normalize_and_quantize_weights",
                    "input_node_count": len(node_ids),
                    "input_node_ids": node_ids[:10]  # First 10 for reference
                })
                
                # Call original function
                formatted_ids, formatted_weights = tracer.original_functions['normalize_and_quantize'](node_ids, node_weights)
                
                tracer.log_weights("NORMALIZATION_OUTPUT", formatted_weights, {
                    "function": "_normalize_and_quantize_weights",
                    "output_node_count": len(formatted_ids),
                    "output_node_ids": formatted_ids[:10],  # First 10 for reference
                    "quantized": True
                })
                
                return formatted_ids, formatted_weights
            
            # Create traced version of _send_weights_to_chain
            def traced_send_weights_to_chain(*args, **kwargs):
                # Extract known parameters for logging
                substrate = args[0] if len(args) > 0 else kwargs.get('substrate')
                keypair = args[1] if len(args) > 1 else kwargs.get('keypair')
                node_ids = args[2] if len(args) > 2 else kwargs.get('node_ids', [])
                node_weights = args[3] if len(args) > 3 else kwargs.get('node_weights', [])
                netuid = args[4] if len(args) > 4 else kwargs.get('netuid', 0)
                version_key = args[5] if len(args) > 5 else kwargs.get('version_key', 0)
                wait_for_inclusion = args[6] if len(args) > 6 else kwargs.get('wait_for_inclusion', False)
                wait_for_finalization = args[7] if len(args) > 7 else kwargs.get('wait_for_finalization', False)
                
                tracer.log_weights("RPC_PREPARATION", node_weights, {
                    "function": "_send_weights_to_chain",
                    "netuid": netuid,
                    "version_key": version_key,
                    "node_count": len(node_ids) if node_ids else 0,
                    "wait_for_inclusion": wait_for_inclusion,
                    "wait_for_finalization": wait_for_finalization,
                    "total_args": len(args),
                    "kwargs": list(kwargs.keys())
                })
                
                # Call original function with all arguments
                result = tracer.original_functions['send_weights_to_chain'](*args, **kwargs)
                
                tracer.log_weights("RPC_COMPLETED", node_weights, {
                    "function": "_send_weights_to_chain",
                    "result_type": type(result),
                    "result_success": result[0] if isinstance(result, tuple) else result,
                    "result_message": result[1] if isinstance(result, tuple) and len(result) > 1 else "No message"
                })
                
                return result
            
            # Apply patches
            fiber_weights.set_node_weights = traced_set_node_weights
            fiber_weights._normalize_and_quantize_weights = traced_normalize_and_quantize
            fiber_weights._send_weights_to_chain = traced_send_weights_to_chain
            
            self.is_patched = True
            print("✅ [RUNTIME_TRACER] Successfully patched fiber functions")
            print(f"📝 [RUNTIME_TRACER] Weight flow will be logged to: {self.log_file}")
            return True
            
        except Exception as e:
            print(f"❌ [RUNTIME_TRACER] Failed to patch fiber functions: {e}")
            traceback.print_exc()
            return False
    
    def remove_patches(self):
        """Remove patches and restore original functions"""
        if not self.is_patched:
            return
            
        try:
            import fiber.chain.weights as fiber_weights
            
            if 'set_node_weights' in self.original_functions:
                fiber_weights.set_node_weights = self.original_functions['set_node_weights']
            if 'normalize_and_quantize' in self.original_functions:
                fiber_weights._normalize_and_quantize_weights = self.original_functions['normalize_and_quantize']
            if 'send_weights_to_chain' in self.original_functions:
                fiber_weights._send_weights_to_chain = self.original_functions['send_weights_to_chain']
            
            self.is_patched = False
            print("✅ [RUNTIME_TRACER] Patches removed and original functions restored")
            
        except Exception as e:
            print(f"❌ [RUNTIME_TRACER] Error removing patches: {e}")

# Global tracer instance
tracer = RuntimeWeightTracer()

def enable_weight_tracing():
    """Enable weight tracing - call this before weight operations"""
    print("🚀 [RUNTIME_TRACER] Enabling weight tracing...")
    return tracer.patch_fiber_functions()

def disable_weight_tracing():
    """Disable weight tracing - call this to clean up"""
    print("🔄 [RUNTIME_TRACER] Disabling weight tracing...")
    tracer.remove_patches()

def test_tracing():
    """Test if tracing is working"""
    try:
        print("🧪 [RUNTIME_TRACER] Testing tracing setup...")
        
        # Test fiber import
        import fiber.chain.weights as fiber_weights
        print(f"✅ [RUNTIME_TRACER] Fiber imported from: {fiber_weights.__file__}")
        
        # Test function access
        print(f"✅ [RUNTIME_TRACER] set_node_weights: {fiber_weights.set_node_weights}")
        print(f"✅ [RUNTIME_TRACER] _normalize_and_quantize_weights: {fiber_weights._normalize_and_quantize_weights}")
        print(f"✅ [RUNTIME_TRACER] _send_weights_to_chain: {fiber_weights._send_weights_to_chain}")
        
        return True
        
    except Exception as e:
        print(f"❌ [RUNTIME_TRACER] Test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("RUNTIME WEIGHT TRACER")
    print("=" * 60)
    
    print("Choose an option:")
    print("1. Test tracing setup")
    print("2. Enable tracing (patches functions)")
    print("3. Disable tracing (restores functions)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        print("\n🧪 Testing tracing setup...")
        if test_tracing():
            print("\n✅ Tracing setup test passed!")
        else:
            print("\n❌ Tracing setup test failed!")
            
    elif choice == "2":
        print("\n🔍 Enabling weight tracing...")
        if enable_weight_tracing():
            print("\n✅ Weight tracing enabled!")
            print("📋 Your validator will now trace weights through the fiber library")
            print("📝 Check logs at: weight_trace_runtime.log")
        else:
            print("\n❌ Failed to enable tracing!")
            
    elif choice == "3":
        print("\n🔄 Disabling weight tracing...")
        disable_weight_tracing()
        print("\n✅ Weight tracing disabled!")
        
    else:
        print("❌ Invalid choice") 