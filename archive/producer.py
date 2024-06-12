import requests
import time
from datetime import datetime

# Sleep for 30 seconds before starting the process
time.sleep(30)

# Flag to skip the first line (header) in the CSV file
first = True

# Open the CSV file in read mode
with open("archive/job_descriptions_streaming.csv", "r") as csv_file:
    # Read the file line by line
    for line in csv_file:
        # Skip the first line (header)
        if first:
            first = False
            continue
        # Strip whitespace and split the line by commas
        line = line.strip().split(",")
        # Append the current timestamp to the line
        line.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # Join the line back into a comma-separated string
        line = ",".join(line)
        # Send the line to the server via a POST request
        response = requests.post('http://10.0.9.25:5050', data=line)
        # Print the status code of the response
        print(response.status_code)
        # Sleep for 2 seconds before processing the next line
        time.sleep(2)
