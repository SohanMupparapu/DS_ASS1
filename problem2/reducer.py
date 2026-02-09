#!/usr/bin/env python3
import sys
import os

DAMPING = int (os.environ("DAMPING"))
N = int(os.environ["TOTAL_NODES"])

current_node = None
sum_contrib = 0.0
adj_list = ""

def emit(node, sum_contrib, adj_list):
    new_pr = (1 - DAMPING) / N + DAMPING * sum_contrib
    print(f"{node}\t{new_pr:.6f}\t{adj_list}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    node, rest = line.split("\t", 1)
    tag, value = rest.split(" ", 1)

    if current_node is None:
        current_node = node

    if node != current_node:
        emit(current_node, sum_contrib, adj_list)
        current_node = node
        sum_contrib = 0.0
        adj_list = ""

    if tag == "PR":
        # NEW PART: parse comma-separated values
        pr_values = value.split(",")
        for pr in pr_values:
            sum_contrib += float(pr)

    elif tag == "ADJ":
        adj_list = value

if current_node is not None:
    emit(current_node, sum_contrib, adj_list)
