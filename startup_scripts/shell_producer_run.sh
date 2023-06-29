#!/bin/bash



echo "Run the Ingestion Script by typing in this format: ./shell_producer_run.sh -c "Austin" -s "TX""


cd ../kafka/kafka-python-code


# Default values
city="Austin"
state="TX"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -c|--city)
            city="$2"
            shift
            shift
            ;;
        -s|--state)
            state="$2"
            shift
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run the Python script with the specified city and state
python producer.py "$city" "$state"
