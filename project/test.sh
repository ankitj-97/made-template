#!/bin/sh

# execute the pipeline
echo "Execute the pipeline"
python ./project/pipeline.py

# test of pipeline
echo "Test if pipeline works correctly"
pytest ./project/test.py
