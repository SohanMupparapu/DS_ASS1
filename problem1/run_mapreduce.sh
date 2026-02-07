if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_dir>"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_DIR=$2

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Make Python scripts executable
chmod +x "$SCRIPT_DIR/mapper.py"
chmod +x "$SCRIPT_DIR/reducer.py"
chmod +x "$SCRIPT_DIR/combiner.py"

hadoop fs -rm -r "$OUTPUT_DIR" 2>/dev/null

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "$SCRIPT_DIR/mapper.py","$SCRIPT_DIR/reducer.py","$SCRIPT_DIR/combiner.py" \
    -mapper "python3 mapper.py" \
    -combiner "python3 combiner.py" \
    -reducer "python3 reducer.py" \
    -input "$INPUT_FILE" \
    -output "$OUTPUT_DIR"

# Check if job succeeded
if [ $? -eq 0 ]; then
    echo "MapReduce job completed successfully"
    echo "Output available at: $OUTPUT_DIR"
else
    echo "MapReduce job failed"
    exit 1
fi