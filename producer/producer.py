import argparse
import csv
import json
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--delay", type=float, default=0.05,
                    help="pause entre deux mesures, en secondes")
parser.add_argument("--start", default="2006-12-16 17:24:00",
                    help="date de départ du rejeu (AAAA-MM-JJ HH:MM:SS)")
parser.add_argument("--limit", type=int, default=0,
                    help="nombre maximal de mesures à envoyer (0 = tout)")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

envoyes = 0
with open("data/household_clean.csv", newline="") as f:
    for row in csv.DictReader(f):
        if row["timestamp"] < args.start:
            continue
        message = {k: (v if k == "timestamp" else float(v)) for k, v in row.items()}
        producer.send("mesures", message)
        envoyes += 1
        if envoyes % 200 == 0:
            print(envoyes, "mesures envoyées, dernière :", row["timestamp"])
        if args.limit and envoyes >= args.limit:
            break
        time.sleep(args.delay)

producer.flush()
print("terminé :", envoyes, "mesures envoyées")