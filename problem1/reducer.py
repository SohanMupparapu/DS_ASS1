import sys
import math
import os
from collections import defaultdict

word_df = defaultdict(int)
TOTAL_DOCS = int(os.environ["TOTAL_DOCS"])


def process_input():
    global total_docs
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, value = line.split('\t')
        except ValueError:
            continue
        try:
            values = list(map(int, value.split(',')))
        except ValueError:
            continue
        else:
            word_df[key] += sum(values)

def emit_output():
    if TOTAL_DOCS == 0:
        return

    for word, df in word_df.items():
        idf = math.log(TOTAL_DOCS / df)
        print(f"{word}\t{idf:.4f}")

def run():
    process_input()
    emit_output()

if __name__ == "__main__":
    run()
