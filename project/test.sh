#!/bin/sh

# execute the pipeline
echo "Execute the pipeline"
python ./data/pipeline.py

# test of pipeline
echo "Test if pipeline works correctly"
pytest ./data/test.py