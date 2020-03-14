"""Configure the git pre-push hook."""

import subprocess
from os.path import join

PRE_PUSH = """#!/bin/bash

HAS_TAGS=0
DRY_RUN=""
VERBOSE=""

## read git push command line arguments
ARGS=`ps -p $PPID -o args=`
for arg in $ARGS; do
    if [[ "$arg" == '--tags' ]] || [[ "$arg" == '--tag' ]] ; then
      HAS_TAGS=1
    fi
    if [[ "$arg" == '--dry-run' ]]; then
	  DRY_RUN="--dry-run"
    fi
    if [[ "$arg" == "-v" ]] || [[ "$arg" == "--verbose" ]]; then
	  VERBOSE='-v'
    fi
done

### AE5 cannot accept annotated tags,
### exit with error
latest_tag=`git describe --tags --abbrev=0`
tag_type=`git cat-file -t $latest_tag`
if [[ "$tag_type" == "tag" ]]; then
    echo "The tag $latest_tag is an annotated tag."
    echo "It must be replaced with a lightweight tag"
    echo "on the local clone and remote repository."
    exit 1
fi

if [ "$HAS_TAGS" -eq "1" ]; then
    ## silent exit on git push --tags
    exit 0
else
    git push --tags
    ae5 post revision-metadata
fi
"""


def install_prepush():
    git_dir = subprocess.check_output('git rev-parse --git-dir', shell=True).decode().strip()

    pre_push_script = join(git_dir, 'hooks', 'pre-push')
    with open(pre_push_script, 'wt') as f:
        f.write(PRE_PUSH)

    subprocess.check_call(f'chmod +x {pre_push_script}', shell=True)
