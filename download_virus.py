#!/usr/bin/env python 

import csv
from collections import defaultdict
import subprocess
import random
import json
import os

def download_accession(org_name, ncbi_accessions, directory, threads=4):
    try:
        cmd = ["ncbi-acc-download", "--format", "fasta", "--out", "/dev/stdout"]
        cmd.extend(ncbi_accessions)
        print(f"\nRunning: {' '.join(cmd)}")
        proc1 = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
        
        cmd = ["grep", "."]
        #print(f"\nRunning: {' '.join(cmd)}")
        proc2 = subprocess.run(cmd, input=proc1.stdout, stdout=subprocess.PIPE, check=True)
    
        cmd = ["bgzip"]
        #print(f"\nRunning: {' '.join(cmd)}")
        handle = open(f"{directory}/{org_name.replace(' ','_').replace('/','_')}.fa.gz", "w")
        proc3 = subprocess.run(cmd, input=proc2.stdout, stdout=handle, check=True)
        handle.close()
        return True
    except:
        print("FAILED, skip")
        return False

def download(organism, max_per_organism = 10):
    os.makedirs("refs", exist_ok=True)
    downloaded = defaultdict(list)
    for key in organism:
        acc_list = random.sample(organism[key]["complete"],min(max_per_organism, len(organism[key]["complete"])))
        if len(acc_list) < max_per_organism:
            acc_list.extend(random.sample(organism[key]["partial"],min(max_per_organism-len(acc_list), len(organism[key]["partial"]))))
        status = download_accession(key, acc_list, "refs")
        if status:
            downloaded[key] = acc_list
    return downloaded

organism = defaultdict(lambda: defaultdict(list))
summary = defaultdict(lambda: defaultdict(int))
with open('test.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        org = row['Organism_Name'].split(" (")[0]
        organism[org][row["Nuc_Completeness"]].append(row['Accession'])
        for key in ['Nuc_Completeness', 'Geo_Location', 'Species', 'Genus', 'Family']:
            summary[key][row[key]] += 1

downloaded = download(organism, 10)

with open("refs.json", "w") as outfile: 
    json.dump(downloaded, outfile)
