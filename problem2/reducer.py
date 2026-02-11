import sys
import os

DAMPING = float(os.environ["DAMPING"])
N = int(os.environ["TOTAL_NODES"])

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 3:
        continue

    # Format: node \t adj_list \t pr_values_list
    node = parts[0]
    adj_list = parts[1]
    pr_values_str = parts[2]

    # Sum all PR contributions
    pr_sum = 0.0
    if pr_values_str:
        pr_values = pr_values_str.split(",")
        for pr_val in pr_values:
            pr_sum += float(pr_val)

    # Compute new PageRank
    new_pr = (1 - DAMPING) / N + DAMPING * pr_sum

    # Output for next iteration: node \t pr \t adj_list
    print(f"{node}\t{new_pr:.6f}\t{adj_list}")
