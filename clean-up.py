import subprocess
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# List of scripts you want to execute
scripts = [
    'breeders-update.py',
    'grooms-update.py',
	'horses-update.py',
	'workouts-update.py',
	'starting-gate-stall-parts-state-logs-update.py',
]

general_logger.info("\nStarting cleanup...")

# Loop through the scripts and execute each one
for script in scripts:
    general_logger.info(f"Executing {script}...")
    try:
        # Execute the script
        completed_process = subprocess.run(['python', script], check=True, text=True, capture_output=True)

        # Print the standard output of the script
        general_logger.info(f"Completed processing {script}")

    except subprocess.CalledProcessError as e:
        # If an error occurs during script execution, print the error
        error_logger.error(f"Error executing {script}: {e}")

        # Optionally, print the standard error output
        error_logger.error(e.stderr)