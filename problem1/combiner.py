import sys
from collections import defaultdict

counts = defaultdict(int)
total_docs = 0

def process_input():
    global total_docs
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, value = line.split('\t')
            value = int(value)
        except ValueError:
            continue

        if key == "__DOC_COUNT__":
            total_docs += value
        else:
            counts[key] += value

def emit_output():
    print(f"__DOC_COUNT__\t{total_docs}")
    for key, value in counts.items():
        print(f"{key}\t{value}")

def main():
    process_input()
    emit_output()

if __name__ == "__main__":
    main()
