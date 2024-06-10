import requests
import time
from datetime import datetime

# Apre il file CSV in modalità lettura
time.sleep(5)
first = True
with open("archive/job_descriptions_streaming.csv", "r") as csv_file:
    # Legge il file riga per riga
    for line in csv_file:
        if first:
            first = False
            continue
        line = line.strip().split(",")        
        line.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        line = ",".join(line)
        # Invia ogni riga al server
        response = requests.post('http://10.0.9.25:5050', data=line)
        print(response.status_code)
        time.sleep(2)
    