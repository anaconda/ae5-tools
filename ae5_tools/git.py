"""Configure the git pre-push hook."""

import subprocess
from os.path import join

PRE_PUSH = """#!/bin/bash

HAS_TAGS=0
DRY_RUN=""
VERBOSE=""

{}

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


def install_prepush(directory=None, external_git=False):
    if not external_git:
        _PRE_PUSH = PRE_PUSH.format('ae5 git config')
    else:
        _PRE_PUSH = PRE_PUSH.format('')

    if directory is None:
        directory = subprocess.check_output('git rev-parse --git-dir', shell=True).decode().strip()
    else:
        directory = join(directory, '.git')

    pre_push_script = join(directory, 'hooks', 'pre-push')
    with open(pre_push_script, 'wt') as f:
        f.write(_PRE_PUSH)

    subprocess.check_call(f'chmod +x {pre_push_script}', shell=True)
