import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')

    # Iteration 1 input: node<TAB>nbr1,nbr2,...
    if len(parts) == 2:
        node = parts[0]
        neighbors = parts[1]
        rank = None

    # Later iterations: node<TAB>rank<TAB>nbr1,nbr2,...
    elif len(parts) == 3:
        node = parts[0]
        rank = float(parts[1])
        neighbors = parts[2]

    else:
        continue

    # Emit adjacency list
    print(f"{node}\tADJ\t{neighbors}")

    # Emit PageRank contributions if rank exists
    if rank is not None and neighbors:
        nbrs = neighbors.split(',')
        outdeg = len(nbrs)
        contrib = rank / outdeg

        for nbr in nbrs:
            print(f"{nbr}\tPR\t{contrib}")
