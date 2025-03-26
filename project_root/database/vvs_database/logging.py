import threading
import logging
import sys
from contextlib import contextmanager

# Thread-local storage for logger instances
_local = threading.local()

def _init_default_logger(name="vvs_database"):
    """Initialize the default logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Only add handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def get_logger():
    """Get the current logger instance."""
    if not hasattr(_local, "logger"):
        _local.logger = _init_default_logger()
    return _local.logger

def set_logger(custom_logger):
    """Set a custom logger (e.g., Dagster's logger)."""
    _local.logger = custom_logger

def reset_logger():
    """Reset to the default logger."""
    if hasattr(_local, "logger"):
        delattr(_local, "logger")

@contextmanager
def use_logger(custom_logger):
    """Context manager to temporarily use a different logger."""
    old_logger = get_logger() if hasattr(_local, "logger") else None
    set_logger(custom_logger)
    try:
        yield
    finally:
        if old_logger:
            set_logger(old_logger)
        else:
            reset_logger()

# Convenience functions for common log levels
def debug(message, *args, **kwargs):
    get_logger().debug(message, *args, **kwargs)

def info(message, *args, **kwargs):
    get_logger().info(message, *args, **kwargs)

def warning(message, *args, **kwargs):
    get_logger().warning(message, *args, **kwargs)

def error(message, *args, **kwargs):
    get_logger().error(message, *args, **kwargs)

def critical(message, *args, **kwargs):
    get_logger().critical(message, *args, **kwargs)

def exception(message, *args, **kwargs):
    """Log an exception with traceback."""
    get_logger().exception(message, *args, **kwargs)
