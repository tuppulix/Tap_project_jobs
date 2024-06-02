import requests
import time
# Apre il file CSV in modalità lettura
time.sleep(5)
with open("archive/job_descriptions_subset.csv", "r") as csv_file:
    # Legge il file riga per riga
    for line in csv_file:
        # Invia ogni riga al server
        response = requests.post('http://10.0.9.25:5050', data=line)
        print(response.status_code)
        time.sleep(0.01)
    