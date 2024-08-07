#!/usr/bin/env python3
"""
Fetch data from pmacct, send it to opensearch.

Copyright (C) 2024 Canonical Ltd.
Author: Laurent Sesquès <laurent.sesques@canonical.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License version 3,
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranties of
MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys
import json
import requests
from datetime import datetime, timedelta

# Opensearch Configuration
OPENSEARCH_HOST = '<replace_me>'
OPENSEARCH_PORT = 9200
OPENSEARCH_AUTH = ('<replace_me>', '<replace_me>')
OPENSEARCH_CERT='/home/ubuntu/opensearch.pem'

# Set the index name with format sflow-YYYY.MM.DD
now = datetime.utcnow()
INDEX_NAME = f"sflow-{now.strftime('%Y.%m.%d')}"
FULL_INDEX_URL = f"https://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{INDEX_NAME}"

TIMESTAMP = now.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

# How many lines to send to ES in bulk
BULK_SIZE = 1000

# Data retention
RETENTION_DAYS = 9

# Create the index if it doesn't already exist
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
response = requests.head(f"{FULL_INDEX_URL}", auth=OPENSEARCH_AUTH, verify=False)
if response.status_code == 404:
    try:
        create_index_response = requests.put(f"{FULL_INDEX_URL}", json={"settings": {}, "mappings": {}}, auth=OPENSEARCH_AUTH, verify=False)
        create_index_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error creating index : {e}")

# Send data to opensearch
def send_to_opensearch(data):
    url = f"{FULL_INDEX_URL}/_bulk"
    headers = {'Content-Type': 'application/json'}
    payload = '\n'.join(data) + '\n'
    response = requests.post(url, headers=headers, data=payload, auth=OPENSEARCH_AUTH, verify=False)
    if response.status_code != 200:
        print("Error sending to Opensearch:", response.text)

# Purge old indices
def purge_old_indices():
    indices_url = f"https://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/_cat/indices?s=index"
    try:
        response = requests.get(indices_url, auth=OPENSEARCH_AUTH, verify=False)
        response.raise_for_status()
        indices = response.text.splitlines()
        days_ago = now - timedelta(days=RETENTION_DAYS)
        for index in indices:
            index_name = index.split()[2]  # Assuming the index name is the third column
            if index_name.startswith('sflow-'):
                try:
                    index_date = datetime.strptime(index_name.split('-')[1], '%Y.%m.%d')
                    if index_date < days_ago:
                        delete_url = f"https://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/{index_name}"
                        delete_response = requests.delete(delete_url, auth=OPENSEARCH_AUTH, verify=False)
                        if delete_response.status_code == 200:
                            print(f"Deleted index {index_name}")
                        else:
                            print(f"Failed to delete index {index_name}: {delete_response.text}")
                except ValueError as e:
                    print(f"Error parsing date from index name {index_name}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving indices: {e}")

# Purge old indices before proceeding
purge_old_indices()

# Read lines from stdin, group and send to Opensearch
lines = []
for line in sys.stdin:
    try:
        json_data = json.loads(line.strip())
        json_data["@timestamp"] = TIMESTAMP
        # Add Opensearch header for bulk actions, indicating that the next line will be indexed
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(json_data))
    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)
        continue

    if len(lines) >= BULK_SIZE:
        send_to_opensearch(lines)
        lines = []

# Send remaining lines
if lines:
    send_to_opensearch(lines)
