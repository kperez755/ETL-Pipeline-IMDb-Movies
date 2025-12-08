import logging
import os

def get_logger(name: str, log_file: str, level=logging.INFO):
    # Ensure log directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if not logger.handlers:

        # File handler
        file_handler = logging.FileHandler(f"{log_file}.log")
        file_handler.setLevel(level)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Format logs
        formatter = logging.Formatter(
            "%(asctime)s — %(name)s — %(levelname)s — %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
