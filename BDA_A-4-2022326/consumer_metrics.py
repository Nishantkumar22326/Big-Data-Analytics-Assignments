import argparse
import time
import csv
from collections import deque, defaultdict
from kafka import KafkaConsumer

GROUND_TRUTH_NODES = 7115
GROUND_TRUTH_EDGES = 103689

def main():
    parser = argparse.ArgumentParser(description="Kafka consumer for streaming wiki-vote metrics")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", default="wiki-vote",
                        help="Kafka topic name (default: wiki-vote)")
    parser.add_argument("--group-id", default="wikivote-metrics-group",
                        help="Consumer group id (default: wikivote-metrics-group)")
    parser.add_argument("--window-seconds", type=int, default=5,
                        help="Sliding window size for edges/sec (default: 5)")
    parser.add_argument("--log-file", default="metrics_log.csv",
                        help="CSV file to log metrics (default: metrics_log.csv)")
    parser.add_argument("--print-interval", type=int, default=1000,
                        help="Print metrics every N edges (default: 1000)")
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    nodes = set()
    edges_count = 0

    # For degree distribution approximation
    out_degree = defaultdict(int)
    in_degree = defaultdict(int)

    # For edges/s sliding window
    timestamps = deque()

    start_time = time.time()
    last_print_time = start_time

    # Open CSV log file
    with open(args.log_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "wall_time",
            "elapsed_sec",
            "nodes",
            "edges",
            "edges_per_sec_window",
            "max_out_degree",
            "max_in_degree"
        ])

        print("Starting consumer loop...")
        for msg in consumer:
            now = time.time()
            elapsed = now - start_time

            try:
                src, tgt = msg.value.split(",")
            except ValueError:
                # malformed message; skip
                continue

            nodes.add(src)
            nodes.add(tgt)
            edges_count += 1

            # Degree statistics
            out_degree[src] += 1
            in_degree[tgt] += 1

            # Sliding window: count messages in last window_seconds
            timestamps.append(now)
            while timestamps and (now - timestamps[0]) > args.window_seconds:
                timestamps.popleft()
            edges_per_sec = len(timestamps) / args.window_seconds if args.window_seconds > 0 else 0.0

            max_out = max(out_degree.values()) if out_degree else 0
            max_in = max(in_degree.values()) if in_degree else 0

            # Log to CSV
            writer.writerow([
                now,
                elapsed,
                len(nodes),
                edges_count,
                edges_per_sec,
                max_out,
                max_in
            ])

            # Periodic console output
            if edges_count % args.print_interval == 0:
                print(
                    f"[{edges_count} edges] Nodes={len(nodes)}, "
                    f"edges/sec({args.window_seconds}s)={edges_per_sec:.2f}, "
                    f"max_out={max_out}, max_in={max_in}"
                )

            # Ground truth check
            if len(nodes) == GROUND_TRUTH_NODES and edges_count == GROUND_TRUTH_EDGES:
                print(
                    f"Reached ground truth: nodes={len(nodes)}, edges={edges_count} "
                    f"in {elapsed:.2f} seconds."
                )
                # You may break here if you want to stop once all data is consumed
                # break

            # Flush periodically to avoid losing data on crash
            if edges_count % 1000 == 0:
                csvfile.flush()

if __name__ == "__main__":
    main()
