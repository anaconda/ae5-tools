import fnmatch
import os
import re
import subprocess
import tarfile


def _list_project(project_directory):
    anchors, nonanchors = [".git/"], []

    gitdir = os.path.join(project_directory, ".git")
    if os.path.exists(gitdir):
        output = subprocess.check_output(["git", "ls-files", "--others", "--ignored", "--exclude-standard", "--directory"], cwd=project_directory)
        anchors.extend(output.decode("utf-8").splitlines())

    igfile = os.path.join(project_directory, ".projectignore")
    if os.path.exists(igfile):
        with open(igfile, "r") as fp:
            for line in fp:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith(r"\#"):
                    line = line[1:]
                pattern = fnmatch.translate(line.lstrip("/"))[4:-3]
                pattern = re.sub(r"(?<!\\)[.]", "[^/]", pattern)
                if line.endswith("/"):
                    pattern = pattern + r".*"
                if line.startswith("/"):
                    anchors.append(pattern)
                else:
                    nonanchors.append(pattern)

    if anchors or nonanchors:
        if nonanchors:
            nonanchors = nonanchors[0] if len(nonanchors) == 1 else "(?:" + "|".join(nonanchors) + ")"
            anchors.append(r"(?:.*/)?" + nonanchors)
        anchors = anchors[0] if len(anchors) == 1 else "(?:" + "|".join(anchors) + ")"
        pattern = r"\A" + anchors + r"(?:/.*)?\Z"
    else:
        pattern = "^$"
    regex = re.compile(pattern)

    for root, dirs, files in os.walk(project_directory):
        filtered_dirs = []
        filtered = False
        for d in dirs:
            abspath = os.path.join(root, d)
            relpath = os.path.relpath(abspath, project_directory)
            if not regex.match(relpath + "/"):
                filtered_dirs.append(d)
        dirs[:] = filtered_dirs
        for f in files:
            abspath = os.path.join(root, f)
            relpath = os.path.relpath(abspath, project_directory)
            if not regex.match(relpath):
                yield (abspath, relpath)


def create_tar_archive(project_directory, arcname, fp):
    with tarfile.open(fileobj=fp, mode="w|gz") as tf:
        for abspath, relpath in _list_project(project_directory):
            tf.add(abspath, os.path.join(arcname, relpath))
