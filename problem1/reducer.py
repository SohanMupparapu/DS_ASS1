import sys
import math


class Reducer:
    def __init__(self):
        self.total_docs = 0
        self.word_df = {}

    def process_input(self):
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                key, value = line.split('\t')
                value = int(value)
            except ValueError:
                continue  # skip malformed lines

            if key == "__DOC_COUNT__":
                self.total_docs += value
            else:
                self.word_df[key] = self.word_df.get(key, 0) + value

    def emit_output(self):
        if self.total_docs == 0:
            return

        for word, df in self.word_df.items():
            idf = math.log(self.total_docs / df)
            print(f"{word}\t{idf:.4f}")

    def run(self):
        self.process_input()
        self.emit_output()


if __name__ == "__main__":
    Reducer().run()
