#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Use this script to reformat a corpus created without grouping field to the 
common format.
"""

import argparse

parser = argparse.ArgumentParser(description='converts the old format for '
        'corpora without grouping field to the new format')
parser.add_argument('-o', '--output-directory', required=True, 
        help='Output directory')
parser.add_argument('-i', '--input-directory', required=True, 
        help='Input directory')
parser.add_argument('-s', '--subreddit', help='subreddit used for this corpus')
args = parser.parse_args()

import os 
import glob
from tqdm import tqdm

files = sorted(glob.glob(f'{args.input_directory}/*.json'))
for f in tqdm(files): 
    filename = os.path.basename(f)
    author = os.path.splitext(filename)[0]
    out_dir = os.path.join(args.output_directory, author, args.subreddit)
    if not os.path.isdir(out_dir): 
        os.makedirs(out_dir)
    with open(f) as i_f:
        for i, comment in enumerate(i_f):
            out_file = os.path.join(out_dir, f'{i:06d}.json')
            with open(out_file, 'w') as o_f:
                o_f.write(comment)

