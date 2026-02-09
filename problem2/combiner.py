import sys

current_node = None
pr_sum = 0.0
adj_list = None   # use None to distinguish empty vs missing

def emit(node, pr_sum, adj_list):
    if pr_sum > 0.0:
        print(f"{node}\tPR\t{pr_sum}")
    if adj_list is not None:
        print(f"{node}\tADJ\t{adj_list}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) < 3:
        continue   # defensive: skip malformed lines

    node, tag, value = parts[0], parts[1], parts[2]

    if current_node is None:
        current_node = node

    if node != current_node:
        emit(current_node, pr_sum, adj_list)
        current_node = node
        pr_sum = 0.0
        adj_list = None

    if tag == "PR":
        pr_sum += float(value)
    elif tag == "ADJ":
        adj_list = value

# flush last key
if current_node is not None:
    emit(current_node, pr_sum, adj_list)
