import sys
import os

TOTAL_NODES = int(os.environ["TOTAL_NODES"])

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")

    # -------- ITERATION 1 --------
    if len(parts) == 2:
        node, adj = parts
        pr = 1.0 / TOTAL_NODES
        
        # Emit adjacency list
        print(f"{node}\tADJ\t{adj}")
        
        # Emit PR contributions
        neighbors = adj.split(",") if adj else []
        if neighbors:
            contrib = pr / len(neighbors)
            for n in neighbors:
                print(f"{n}\tPR\t{contrib}")
        continue

    # -------- ITERATION >= 2 --------
    if len(parts) == 3:
        node, pr, adj = parts
        pr = float(pr)
        
        # Emit adjacency list
        print(f"{node}\tADJ\t{adj}")
        
        # Emit PR contributions
        neighbors = adj.split(",") if adj else []
        if neighbors:
            contrib = pr / len(neighbors)
            for n in neighbors:
                print(f"{n}\tPR\t{contrib}")