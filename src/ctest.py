import os
from datetime import datetime

# Get the current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the filename
cwd = os.getcwd()
filename = f"{cwd}/{timestamp}.txt"

# Create an empty file with the timestamp as the filename
with open(filename, "w") as file:
    pass  # Create the file without writing anything to it

print(f"Empty file created: {filename}")
