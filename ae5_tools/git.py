"""Configure the git pre-push hook."""

import subprocess
from os.path import join

PRE_PUSH = """#!/bin/bash

HAS_TAGS=0
DRY_RUN=""
VERBOSE=""

ae5 git config

## read git push command line arguments
push_command=$(ps -ocommand= -p $PPID)

with_tags="^(--tags|--follow-tags)"
is_dry_run="^(--dry-run|-n)"
is_destructive='^(--force|--delete|-f|-d)'
is_verbose="^(--verbose|-v)"

for arg in $push_command; do
  if [[ $arg =~ $is_destructive ]]; then
    echo "Force push and delete are disabled. You may wish to use 'git revert' instead."
    exit 1
  fi
  if [[ $arg =~ $is_dry_run ]]; then
    DRY_RUN="--dry-run"
  fi
  if [[ $arg =~ $is_verbose ]]; then
    VERBOSE="-v"
  fi
  if [[ $arg =~ $with_tags ]]; then
    HAS_TAGS=1
  fi
done

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
