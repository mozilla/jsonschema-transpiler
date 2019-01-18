#!/usr/bin/env python3
import argparse
import json
import logging
import os
import shutil


def parse_arguments():
    parser = argparse.ArgumentParser(description="format json documents")
    parser.add_argument(
        "--path",
        type=str,
        default=os.path.realpath(
            os.path.join(os.path.dirname(__file__), "../tests/resources")
        ),
        help="Directory path to json documents to format",
    )
    parser.add_argument("--backup", action="store_true", help="Create backup file")
    return parser.parse_args()


def format_document(filename, backup=False):
    if backup:
        backup_filename = filename + ".bak"
        shutil.copyfile(filename, backup_filename)
    logging.info("Formatting document {}".format(filename))
    with open(filename, "r") as f:
        data = json.load(f)
    with open(filename, "w") as f:
        json.dump(data, f, indent=4, sort_keys=True)
        f.write("\n")


def main():
    args = parse_arguments()
    logging.info(
        "Running format script with path {} and backups={}".format(
            args.path, args.backup
        )
    )
    for root, _, files in os.walk(args.path):
        for name in files:
            if not name.endswith(".json"):
                continue
            path = os.path.join(root, name)
            format_document(path, args.backup)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
