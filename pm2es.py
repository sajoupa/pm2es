#!/usr/bin/env python3
"""
Fetch data from pmacct, send it to elasticsearch.

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
from datetime import datetime

# Configuration Elasticsearch
ELASTICSEARCH_HOST = '<REPLACE_ME>'
ELASTICSEARCH_PORT = 9200

# Set the index name with format sflow-YYYY.MM.DD
now = datetime.utcnow()
INDEX_NAME = f"sflow-{now.strftime('%Y.%m.%d')}"
FULL_INDEX_URL = f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}/{INDEX_NAME}"

TIMESTAMP = now.strftime('%Y-%m-%dT%H:%M') + 'Z'

# How many lines to send to ES in bulk
BULK_SIZE = 1000

# Create the index if it doesn't already exist
response = requests.head(f"{FULL_INDEX_URL}")
if response.status_code == 404:
    try:
        create_index_response = requests.put(f"{FULL_INDEX_URL}", json={"settings": {}, "mappings": {}})
        create_index_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error creating index : {e}")

# Send data to elasticsearch
def send_to_elasticsearch(data):
    url = f"{FULL_INDEX_URL}/_bulk"
    headers = {'Content-Type': 'application/json'}
    payload = '\n'.join(data) + '\n'
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code != 200:
        print("Error sending to Elasticsearch:", response.text)

# Read lines from stdin, group and send to Elasticsearch
lines = []
for line in sys.stdin:
    try:
        json_data = json.loads(line.strip())
        json_data["@timestamp"] = TIMESTAMP
        # Add Elasticsearch header for bulk actions, indicating that the next line will be indexed
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(json_data))
    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)
        continue

    if len(lines) >= BULK_SIZE:
        send_to_elasticsearch(lines)
        lines = []

# Send remaining lines
if lines:
    send_to_elasticsearch(lines)
