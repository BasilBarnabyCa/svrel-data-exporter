# logger_setup.py
import logging
import datetime

def setup_loggers():
    # Current date to use in the log filenames
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # General logs setup
    general_log_filename = f"logs/general_{current_date}.log"
    general_logger = logging.getLogger('generalLogger')
    general_logger.setLevel(logging.INFO)  # Set to INFO to capture all general logs
    general_handler = logging.FileHandler(general_log_filename)
    general_handler.setLevel(logging.INFO)
    general_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    general_handler.setFormatter(general_formatter)
    general_logger.addHandler(general_handler)
    stream_handler = logging.StreamHandler()  # Console output
    stream_handler.setLevel(logging.INFO)  # Set the log level for console output
    stream_handler.setFormatter(general_formatter)  # Reuse the formatter from the file handler
    general_logger.addHandler(stream_handler)

    # Error logs setup
    error_log_filename = f"logs/error_{current_date}.log"
    error_logger = logging.getLogger('errorLogger')
    error_logger.setLevel(logging.ERROR)  # Set to ERROR to capture error logs
    error_handler = logging.FileHandler(error_log_filename)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)
    error_stream_handler = logging.StreamHandler()
    error_stream_handler.setLevel(logging.ERROR)  # Only show ERROR and above in the console
    error_stream_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_stream_handler)

    return general_logger, error_logger
