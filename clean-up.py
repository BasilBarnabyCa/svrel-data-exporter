import subprocess

# List of scripts you want to execute
scripts = [
    'breeders-type-update.py',
    'is-banned-update.py',
]

# Loop through the scripts and execute each one
for script in scripts:
    print(f"Starting cleanup...")
    print(f"Executing {script}...")
    try:
        # Execute the script
        completed_process = subprocess.run(['python', script], check=True, text=True, capture_output=True)

        # Print the standard output of the script
        print(completed_process.stdout)

    except subprocess.CalledProcessError as e:
        # If an error occurs during script execution, print the error
        print(f"Error executing {script}: {e}")

        # Optionally, print the standard error output
        print(e.stderr)
