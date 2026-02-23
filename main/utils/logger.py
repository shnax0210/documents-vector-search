import logging
import sys

def setup_root_logger(level: int = logging.INFO, use_stderr: bool = False) -> logging.Logger:
    root_logger = logging.getLogger()
    
    if root_logger.handlers:
        return root_logger
    
    root_logger.setLevel(level)
    
    stream = sys.stderr if use_stderr else sys.stdout
    console_handler = logging.StreamHandler(stream)
    console_handler.setLevel(level)
    
    # Create formatter for root logger
    formatter = logging.Formatter(
        '%(message)s'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)
    
    # Configure specific third-party library log levels
    configure_third_party_loggers()
    
    return root_logger

def configure_third_party_loggers():
    library_configs = {
        'faiss': logging.INFO,
        'sentence_transformers': logging.INFO,
    }
    
    for lib_name, log_level in library_configs.items():
        logging.getLogger(lib_name).setLevel(log_level) 