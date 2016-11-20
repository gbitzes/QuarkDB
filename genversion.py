#!/usr/bin/env python
import os, subprocess, sys, inspect

def sh(cmd):
    return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)

def getFile(filename):
    try:
        with open(filename) as f:
            content = "".join(f.readlines())
    except:
        return ""

    return content

def main():

    try:
        root_dir = sh("git rev-parse --show-toplevel").strip()
        os.chdir(root_dir)
    except:
        sys.exit(0)

    commit_hash = sh("git rev-parse HEAD").strip()
    git_describe = sh("git describe --dirty").strip()

    git_commit_date = sh("git log -1 --date=short --pretty=format:%cd").strip().replace("-", "")
    branch = sh("git rev-parse --symbolic-full-name --abbrev-ref HEAD").strip()
    latest_tag = sh("git describe --abbrev=0 --tags").strip()
    versions = latest_tag.split(".")

    version_tag = git_describe.split(".")[2]
    dash = version_tag.find("-")
    if dash >= 0:
        version_tag = version_tag[dash+1:]
    else:
        version_tag = "release"

    old = getFile("src/Version.hh")
    template = getFile("src/Version.hh.in")

    replacements = [
      ["@GIT_SHA1@", commit_hash],
      ["@GIT_DESCRIBE@", git_describe],
      ["@GIT_COMMIT_DATE@", git_commit_date],
      ["@GIT_BRANCH@", branch],
      ["@VERSION_MAJOR@", versions[0]],
      ["@VERSION_MINOR@", versions[1]],
      ["@VERSION_PATCH@", versions[2]],
      ["@VERSION_TAG@", version_tag]
    ]

    new = template
    for replacement in replacements:
        new = new.replace(replacement[0], replacement[1])

    if old == new:
        print("Version.hh up-to-date.")
    else:
        with open("src/Version.hh", "w") as f:
            f.write(new)
        print("Version.hh updated. (" + git_describe + ")")

if __name__ == '__main__':
    main()
