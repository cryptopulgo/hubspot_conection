#!/usr/bin/python3

"""
Python script to compress PDF files using ghostscript.
@author Peter Heywood
"""

import os
import subprocess
import argparse
import time

COMPRESSIONS = [
    "screen",
    "ebook",
    "printer",
    "prepress",
    "default",
]
COMPRESSION_DEFAULT = COMPRESSIONS[0]


def process_args():
    parser = argparse.ArgumentParser(description="Compress a PDF using ghostscript.")
    parser.add_argument(
        "input",
        type=str,
        help="Input PDF file"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Optional output path. Path is calculated if not specified."
    )
    parser.add_argument(
        "-c",
        "--compression",
        choices=COMPRESSIONS,
        default=COMPRESSION_DEFAULT,
        help="Compression level. Defaults to '{:}'".format(COMPRESSION_DEFAULT)
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwite output file"
    )
    args = parser.parse_args()
    return args


def human_bytes(bytes):
    FACTOR = 1024.0
    value = bytes
    for prefix in ["B", "KiB", "MiB", "GiB"]:
        if value < FACTOR:
            return "{:.3f}{:}".format(value, prefix)
        value = value / FACTOR
    return "{:.3f}{:}".format(value, prefix)


def compress(inpath, outpath, compression, force):
    # validate input file
    if not os.path.isfile(inpath):
        print("Error: input {:} is not a file".format(inpath))

    # Set an outpath if not provided.
    if outpath is None:
        a, b = os.path.splitext(inpath)
        outpath = "{:}.{:}{:}".format(a, compression, b)

    # Validate output file does not exist.
    if os.path.exists(outpath):
        if os.path.isdir(outpath):
            print("Error: output {:} is a directory".format(outpath))
            return False
        elif os.path.isfile(outpath) and not force:
            print("Error: output {:} already exists".format(outpath))
            return False

    # Log to user
    print("Compressing {:} to {:}. compression={:}...".format(inpath, outpath, compression))

    # Run command
    command_str = 'gswin64 -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS="/{:}" -dNOPAUSE -dQUIET -dBATCH -sOutputFile="{:}" "{:}"'.format(
        compression, outpath, inpath)

    try:
        t0 = time.time()
        result = subprocess.call(command_str, shell=True)
        t1 = time.time()
    except Exception as e:
        print(e)
        return False

    time_elapsed = t1 - t0
    size_in = os.path.getsize(inpath)
    size_out = os.path.getsize(outpath)
    compression_ratio = size_in / size_out

    time_elapsed_str = "{:.3f}s".format(time_elapsed)
    size_in_str = human_bytes(size_in)
    size_out_str = human_bytes(size_out)
    compression_ratio_str = "{:.3f}x".format(compression_ratio)

    # Log completion.
    print(
        "> Completed in {:}, {:} to {:} ({:})".format(
            time_elapsed_str,
            size_in_str,
            size_out_str,
            compression_ratio_str
        )
    )
    return True


def main():
    args = process_args()

    compress(args.input, args.output, args.compression, args.force)


if __name__ == "__main__":
    main()