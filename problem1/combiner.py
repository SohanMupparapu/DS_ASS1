import sys
from collections import defaultdict

counts = defaultdict(int)

def process_input():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, value = line.split('\t')
            value = int(value)
        except ValueError:
            continue

        counts[key] += value

def emit_output():
    for key, value in counts.items():
        print(f"{key}\t{value}")

def main():
    process_input()
    emit_output()

if __name__ == "__main__":
    main()
