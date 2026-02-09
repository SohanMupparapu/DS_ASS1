#!/usr/bin/env python3
import sys

current_node = None
pr_sum = 0.0
adj_list = ""

def emit(node, pr_sum, adj_list):
    if pr_sum != 0.0:
        print(f"{node}\tPR {pr_sum}")
    if adj_list:
        print(f"{node}\tADJ {adj_list}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    node, rest = line.split("\t", 1)
    tag, value = rest.split(" ", 1)

    if current_node is None:
        current_node = node

    if node != current_node:
        emit(current_node, pr_sum, adj_list)
        current_node = node
        pr_sum = 0.0
        adj_list = ""

    if tag == "PR":
        pr_sum += float(value)
    elif tag == "ADJ":
        adj_list = value

if current_node is not None:
    emit(current_node, pr_sum, adj_list)

