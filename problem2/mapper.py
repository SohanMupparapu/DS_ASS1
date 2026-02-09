import sys
import os

TOTAL_NODES = int(os.environ["TOTAL_NODES"])

current_node = None
current_pr = None
current_adj = None

def emit(node, pr, adj):
    # Always emit adjacency
    print(f"{node}\tADJ\t{adj}")

    neighbors = adj.split(",") if adj else []
    if neighbors:
        contrib = pr / len(neighbors)
        for n in neighbors:
            print(f"{n}\tPR\t{contrib}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")

    # -------- ITERATION 1 --------
    if len(parts) == 2:
        node, adj = parts
        pr = 1.0 / TOTAL_NODES
        emit(node, pr, adj)
        continue

    # -------- ITERATION >= 2 --------
    node, tag, value = parts

    if current_node is None:
        current_node = node

    if node != current_node:
        if current_pr is not None and current_adj is not None:
            emit(current_node, current_pr, current_adj)

        current_node = node
        current_pr = None
        current_adj = None

    if tag == "PR":
        current_pr = float(value)
    elif tag == "ADJ":
        current_adj = value

# flush last node
if current_node is not None:
    if current_pr is not None and current_adj is not None:
        emit(current_node, current_pr, current_adj)
