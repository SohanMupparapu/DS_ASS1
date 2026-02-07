import sys
import re

def main():
    count=0
    key_word="__DOC_COUNT__"
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        # Split by tab to get doc_id and document_text
        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue
        
        doc_id = parts[0]
        document_text = parts[1]
        
        words = re.findall(r'\b[a-z]+\b', document_text.lower())
        
        unique_words = set(words)
        
        # Emit each unique word with the document ID
        for word in unique_words:
            print(f"{word}\t1")
        count+=1
    print(f"{key_word}\t{count}")

if __name__ == "__main__":
    main()