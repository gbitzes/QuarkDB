#!/usr/bin/env python
import os, subprocess, sys, inspect, argparse

def sh(cmd):
    return subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)

def getFile(filename):
    try:
        with open(filename) as f:
            content = "".join(f.readlines())
    except:
        return ""

    return content

def applyTemplate(template, target, replacements):
    templateContent = getFile(template)
    oldContent = getFile(target)

    newContent = templateContent
    for replacement in replacements:
        newContent = newContent.replace(replacement[0], replacement[1])

    if oldContent == newContent:
        return False

    with open(target, "w") as f:
        f.write(newContent)
    return True

def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description="Configure files that contain version numbers.\n")
    parser.add_argument('--template', type=str, default="src/Version.hh.in", help="The template file.")
    parser.add_argument('--out', type=str, default="src/Version.hh", help="The file to output.")
    args = parser.parse_args()

    try:
        root_dir = sh("git rev-parse --show-toplevel").strip()
        os.chdir(root_dir)
    except:
        # not a failure - simply means we're building from a release tarball
        print("Cannot regenerate {0} from git".format(args.out))
        sys.exit(0)

    commit_hash = sh("git rev-parse HEAD").strip()
    git_describe = sh("git describe --dirty").strip()

    git_commit_date = sh("git log -1 --date=short --pretty=format:%cd").strip().replace("-", "")
    branch = sh("git rev-parse --symbolic-full-name --abbrev-ref HEAD").strip()
    latest_tag = sh("git describe --abbrev=0 --tags").strip()
    versions = latest_tag.split(".")
    if versions[0].startswith('v'): versions[0] = versions[0][1:]

    build = git_describe.split(".")[2]
    dash = build.find("-")

    release = git_describe.split(".")[2]
    dash = release.find("-")
    if dash >= 0:
        parts = build[dash+1:].replace("-", ".").split(".")
        if len(parts) > 1: parts[1] = parts[1][1:] # remove "g" preceeding the SHA1
        build = ".".join(parts)
        version_full = versions[0] + "." + versions[1] + "." + versions[2] + "." + build
    else:
        build = ""
        version_full = versions[0] + "." + versions[1] + "." + versions[2]

    rocksdb_cache = ""
    try:
        rocksdb_cache = sh("ci/rocksdb-cmake.sh")
    except:
        pass

    replacements = [
      ["@GIT_SHA1@", commit_hash],
      ["@GIT_DESCRIBE@", git_describe],
      ["@GIT_COMMIT_DATE@", git_commit_date],
      ["@GIT_BRANCH@", branch],
      ["@VERSION_MAJOR@", versions[0]],
      ["@VERSION_MINOR@", versions[1]],
      ["@VERSION_PATCH@", versions[2]],
      ["@VERSION_BUILD@", build],
      ["@VERSION_FULL@", version_full],
      ["@ROCKSDB_CACHED_BUILD@", rocksdb_cache]
    ]

    if applyTemplate(args.template, args.out, replacements):
        print("{0} updated. ({1})".format(args.out, version_full))
    else:
        print("{0} up-to-date.".format(args.out))

if __name__ == '__main__':
    main()
