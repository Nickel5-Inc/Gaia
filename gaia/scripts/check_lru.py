# inspect_caches.py
import sys, pkgutil, inspect
from functools import _lru_cache_wrapper

def find_lru_caches(root_pkg):
    sys.path.insert(0, '.')
    for finder, mod_name, ispkg in pkgutil.walk_packages([root_pkg], prefix=root_pkg + "."):
        try:
            mod = __import__(mod_name, fromlist=[""])
        except ImportError:
            continue
        for name, obj in inspect.getmembers(mod, inspect.isfunction):
            if isinstance(obj, _lru_cache_wrapper):
                yield mod_name + "." + name, obj.cache_info()

if __name__ == "__main__":
    root = sys.argv[1] if len(sys.argv) > 1 else "gaia"
    caches = sorted(find_lru_caches(root), key=lambda x: x[1].currsize, reverse=True)
    print(f"{'Function':<50} {'maxsize':>8} {'currsize':>8}")
    print("-"*70)
    for fn, ci in caches:
        mark = " (unbounded)" if ci.maxsize is None else ""
        print(f"{fn:<50} {ci.maxsize!s:>8} {ci.currsize:>8}{mark}")