#!/bin/bash

# Perform the build
conda build conda-recipe --debug --no-anaconda-upload --no-test --output-folder build

# Check the exit status of the cp command
if [ $? -eq 0 ]; then
  echo "Build successful"
else
  echo "Build failed"
fi

echo "Moving on ..."
exit 0
