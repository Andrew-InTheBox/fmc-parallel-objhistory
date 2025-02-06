#!/bin/bash

# Base directory path
BASE_DIR="/home/acb/Client_Work/VaultSpeed/Colruyt/fmc-loading-performance/bv-split"

# Input paths
INPUT_ZIP="${BASE_DIR}/1668_FMC.zip"
CONFIG_FILE="${BASE_DIR}/config-file/ConfigurationFileExample.xlsx"

# Script and output paths
PYTHON_SCRIPT="${BASE_DIR}/src/main.py"
OUTPUT_DIR="${BASE_DIR}/output"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Print the paths being used
echo "Using the following paths:"
echo "Input ZIP: $INPUT_ZIP"
echo "Config File: $CONFIG_FILE"
echo "Python Script: $PYTHON_SCRIPT"
echo "Output Directory: $OUTPUT_DIR"
echo ""

# Execute the Python script
echo "Executing DAG splitter..."
python "$PYTHON_SCRIPT" "$INPUT_ZIP" "$CONFIG_FILE" "$OUTPUT_DIR"

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo "Script completed successfully!"
    echo "Check $OUTPUT_DIR for the output files"
else
    echo "Script failed with error code $?"
fi