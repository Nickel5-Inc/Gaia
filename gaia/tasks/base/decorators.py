from functools import wraps
import time
import traceback
from pydantic import ValidationError


"""
Decorators for task module base classes.
"""


def handle_validation_error(func):
    """
    Decorator for handling pydantic validation errors in base classes.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValidationError as ve:
            print(f"Validation error in {func.__name__}: {ve}")
            print(traceback.format_exc())
            # TODO: Implement system-wide logging instead of print statements
            raise

    return wrapper


def task_timer(func):
    """
    Decorator for timing the execution of a task, subtask, or method.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Task {func.__name__} took {end_time - start_time} seconds to complete.")
        return result

    return wrapper
