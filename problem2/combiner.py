import sys

# HashMap to accumulate PR values
pr_map = {}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 3:
        continue   # defensive: skip malformed lines

    node, tag, value = parts[0], parts[1], parts[2]

    if tag == "PR":
        # Accumulate PR contributions for this node
        pr_map[node] = pr_map.get(node, 0.0) + float(value)
    elif tag == "ADJ":
        # ADJ appears only once per node, emit immediately
        print(f"{node}\tADJ\t{value}")

# After processing all lines, emit accumulated PR values
for node, pr_sum in pr_map.items():
    if pr_sum > 0.0:  # optional: skip if no PR contributions
        print(f"{node}\tPR\t{pr_sum}")