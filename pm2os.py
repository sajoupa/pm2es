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

import ipaddress
import hashlib
import json
import requests
import sys
from datetime import datetime, timedelta

# Choose if we want to pseudonymize data
PSEUDONYMIZE = False

# Opensearch Configuration
TARGETS = [
    {
        "host": "REPLACE_ME",
        "port": 9200,
        "auth": ("REPLACE_ME", "REPLACE_ME"),
    },
    {
        "host": "REPLACE_ME",
        "port": 9200,
        "auth": ("REPLACE_ME", "REPLACE_ME"),
    }
]

# Set the index name with format sflow-YYYY.MM.DD
now = datetime.utcnow()
INDEX_NAME = f"sflow-{now.strftime('%Y.%m.%d')}"
TIMESTAMP = now.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

# How many lines to send to ES in bulk
BULK_SIZE = 1000

# Data retention
RETENTION_DAYS = 9

# Create the index if it doesn't already exist
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_index_if_needed(target):
    full_index_url = f"https://{target['host']}:{target['port']}/{INDEX_NAME}"
    response = requests.head(full_index_url, auth=target['auth'], verify=False)
    if response.status_code == 404:
        try:
            create_index_response = requests.put(
                full_index_url,
                json={"settings": {}, "mappings": {}},
                auth=target['auth'],
                verify=False
            )
            create_index_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error creating index on {target['host']}: {e}")

for target in TARGETS:
    create_index_if_needed(target)

# Send data to opensearch
def send_to_opensearch(data, target):
    url = f"https://{target['host']}:{target['port']}/{INDEX_NAME}/_bulk"
    headers = {'Content-Type': 'application/json'}
    payload = '\n'.join(data) + '\n'
    response = requests.post(url, headers=headers, data=payload, auth=target['auth'], verify=False)
    if response.status_code != 200:
        print(f"Error sending to Opensearch ({target['host']}):", response.text)

# Purge old indices
def purge_old_indices(target):
    indices_url = f"https://{target['host']}:{target['port']}/_cat/indices?s=index"
    try:
        response = requests.get(indices_url, auth=target['auth'], verify=False)
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
                        delete_response = requests.delete(delete_url, auth=target['auth'], verify=False)
                        if delete_response.status_code == 200:
                            print(f"Deleted index {index_name}")
                        else:
                            print(f"Failed to delete index {index_name}: {delete_response.text}")
                except ValueError as e:
                    print(f"Error parsing date from index name {index_name}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving indices: {e}")

# Purge old indices for all targets
for target in TARGETS:
    purge_old_indices(target)

def pseudonymize_ipv4(ip, salt="default_salt"):
    """Pseudonymize an IPv4 address while ensuring it stays valid."""
    # Combine IP with salt and hash it
    hash_bytes = hashlib.sha256((salt + ip).encode()).digest()
    # Use the first 4 bytes to create a valid IPv4 address
    return ".".join(str(byte) for byte in hash_bytes[:4])

def pseudonymize_ipv6(ip, salt="default_salt"):
    """Pseudonymize an IPv6 address while ensuring it stays valid."""
    # Combine IP with salt and hash it
    hash_bytes = hashlib.sha256((salt + ip).encode()).digest()
    # Use the hash to create 8 groups of 16-bit hex values
    pseudonymized_parts = [
        f"{(hash_bytes[i] << 8 | hash_bytes[i+1]) & 0xFFFF:04x}"
        for i in range(0, 16, 2)
    ]
    return ":".join(pseudonymized_parts)

def pseudonymize_ip(ip, salt="default_salt"):
    """Detect and pseudonymize an IP address."""
    try:
        ip_obj = ipaddress.ip_address(ip)
        if isinstance(ip_obj, ipaddress.IPv4Address):
            return pseudonymize_ipv4(ip, salt)
        elif isinstance(ip_obj, ipaddress.IPv6Address):
            return pseudonymize_ipv6(ip, salt)
    except ValueError:
        return f"Invalid IP: {ip}"

# Read lines from stdin, group and send to Opensearch
lines = []
for line in sys.stdin:
    try:
        json_data = json.loads(line.strip())
        json_data["@timestamp"] = TIMESTAMP
        # Pseudonymize IPs
        if PSEUDONYMIZE and "ip_dst" in json_data:
            json_data["ip_dst"] = pseudonymize_ip(json_data["ip_dst"])
        if PSEUDONYMIZE and "ip_src" in json_data:
            json_data["ip_src"] = pseudonymize_ip(json_data["ip_src"])
        # Add Opensearch header for bulk actions, indicating that the next line will be indexed
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(json_data))
    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)
        continue

    if len(lines) >= BULK_SIZE:
        for target in TARGETS:
            send_to_opensearch(lines, target)
        lines = []

# Send remaining lines
if lines:
    for target in TARGETS:
        send_to_opensearch(lines, target)
