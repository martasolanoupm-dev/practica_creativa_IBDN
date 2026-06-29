#!/usr/bin/env python3

import sys, json
from cassandra.cluster import Cluster

cluster = Cluster(["cassandra"])
session = cluster.connect("agile_data_science")

insert_stmt = session.prepare(
    "INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)"
)

path = sys.argv[1] if len(sys.argv) > 1 else "/practica/data/origin_dest_distances.jsonl"
count = 0
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        session.execute(insert_stmt, (record["Origin"], record["Dest"], float(record["Distance"])))
        count += 1

print("Distancias importadas a Cassandra:", count)
cluster.shutdown()