import argparse
import time
import random
from kafka import KafkaProducer

def load_edges(path, skip_comments=True):
    edges = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if skip_comments and line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 2:
                continue
            src, tgt = parts
            edges.append((src, tgt))
    return edges

def main():
    parser = argparse.ArgumentParser(description="Kafka producer for wiki-vote edges")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="wiki-vote",
                        help="Kafka topic name (default: wiki-vote)")
    parser.add_argument("--file", default="Wiki-Vote.txt",
                        help="Path to Wiki-Vote.txt")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Delay between messages in seconds (default: 0.1)")
    parser.add_argument("--shuffle", action="store_true",
                        help="Shuffle edges to simulate out-of-order events")
    args = parser.parse_args()

    print(f"Loading edges from {args.file} ...")
    edges = load_edges(args.file)
    print(f"Loaded {len(edges)} edges")

    if args.shuffle:
        print("Shuffling edges to simulate out-of-order events...")
        random.shuffle(edges)

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)

    try:
        for i, (src, tgt) in enumerate(edges, start=1):
            payload = f"{src},{tgt}".encode("utf-8")
            producer.send(args.topic, payload)
            if i % 1000 == 0:
                print(f"Sent {i} messages")
            time.sleep(args.delay)
    finally:
        print("Flushing and closing producer...")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
