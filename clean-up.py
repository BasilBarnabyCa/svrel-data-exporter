import subprocess
from logger_setup import setup_loggers

# Initialize loggers
general_logger, error_logger = setup_loggers()

# List of scripts you want to execute
scripts = [
    'cleanup_scripts/breeders-update.py',
    'cleanup_scripts/grooms-update.py',
    'cleanup_scripts/drivers-update.py',
	'cleanup_scripts/horses-update.py',
	'cleanup_scripts/horse-microchip-update.py',
    'cleanup_scripts/merge-horses-sires.py',
	'cleanup_scripts/merge-horses-dams.py',
	'cleanup_scripts/horse-duplicate.py',
	'cleanup_scripts/horse-deaths-update.py',
	'cleanup_scripts/horse-health-tracking-update.py',
	'cleanup_scripts/horse-offsite-tracking-update.py',
	'cleanup_scripts/horse-race-duplicates.py',
	'cleanup_scripts/horse-gender-tracking-update.py',
	'cleanup_scripts/workouts-update.py',
	'cleanup_scripts/users-update.py',
	'cleanup_scripts/stalls-update.py',
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