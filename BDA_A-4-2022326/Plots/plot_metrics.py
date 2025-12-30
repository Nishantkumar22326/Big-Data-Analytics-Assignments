import argparse, csv
import matplotlib.pyplot as plt

def read_metrics(log_file):
    elapsed=[]; nodes=[]; edges=[]; eps=[]; max_out=[]; max_in=[]
    with open(log_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            elapsed.append(float(row["elapsed_sec"]))
            nodes.append(int(row["nodes"]))
            edges.append(int(row["edges"]))
            eps.append(float(row["edges_per_sec_window"]))
            max_out.append(int(row["max_out_degree"]))
            max_in.append(int(row["max_in_degree"]))
    return elapsed, nodes, edges, eps, max_out, max_in

def main():
    p = argparse.ArgumentParser(description="Plot streaming metrics from CSV")
    p.add_argument("--log-file", required=True)
    p.add_argument("--prefix", default="metrics")
    a = p.parse_args()

    t, nodes, edges, eps, max_out, max_in = read_metrics(a.log_file)

    plt.figure()
    plt.plot(t, nodes, label="Nodes")
    plt.plot(t, edges, label="Edges")
    plt.xlabel("Time (s)"); plt.ylabel("Count"); plt.title("Nodes and edges over time")
    plt.legend(); plt.tight_layout(); plt.savefig(f"{a.prefix}_nodes_edges.png")

    plt.figure()
    plt.plot(t, eps)
    plt.xlabel("Time (s)"); plt.ylabel("Edges per second (windowed)")
    plt.title("Edges per second over time")
    plt.tight_layout(); plt.savefig(f"{a.prefix}_eps.png")

    plt.figure()
    plt.plot(t, max_out, label="Max out-degree")
    plt.plot(t, max_in, label="Max in-degree")
    plt.xlabel("Time (s)"); plt.ylabel("Degree"); plt.title("Max in/out degree over time")
    plt.legend(); plt.tight_layout(); plt.savefig(f"{a.prefix}_degree.png")

if __name__ == "__main__":
    main()