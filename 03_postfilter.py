#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.


from collections import defaultdict
from tqdm import tqdm
import argparse
import json
import logging
import os
import pathlib

logging.basicConfig(level='INFO', 
                    format='%(asctime)s %(levelname)s: %(message)s')

parser = argparse.ArgumentParser(
        description='Makes a stricter version of a generated Corpus by '
                    're-setting c and m values.')

parser.add_argument(
        '-i', '--input-directory', required=True, 
        help='Input directory')
parser.add_argument(
        '-m', required=True, type=int, 
        help='How many documents must be present in each group per target')
parser.add_argument(
        '-c', required=True, type=int, 
        help='min. length of remaining documents')
parser.add_argument(
        '--max-c', type=int, required=False, default=None,
        help='Optional upper bound for c')
parser.add_argument(
        '-o', '--output-directory', required=False, default=None,
        help='Output directory. If omitted, the directory name will be '
             'selected automatically based on the other parameters.')

args = parser.parse_args()
authors = sorted(os.listdir(args.input_directory))
ignored_authors = set()
if args.output_directory is None: 
    output_name = f'{args.m}_{args.c}'
    if args.max_c is not None:
        output_name += f'_{args.max_c}'
    indir = pathlib.Path(args.input_directory)
    output_directory = os.path.join(indir.parent, output_name)
else:
    output_directory = args.output_directory

for author in tqdm(authors):
    author_dir = os.path.join(args.input_directory, author)
    categories = sorted(os.listdir(author_dir))
    good = defaultdict(list)
    bad = defaultdict(list)
    for category in categories:
        category_dir = os.path.join(author_dir, category)
        posts = sorted(os.listdir(category_dir))

        for post in posts:
            src = os.path.join(category_dir, post)
            with open(src) as i_f:
                js = json.load(i_f)
                size = len(js['body'])
                if args.max_c and size > args.max_c: 
                    bad[category].append(post)
                elif size < args.c:
                    bad[category].append(post)
                else:
                    good[category].append(post)

        if len(good[category]) < args.m:
            ignored_authors.add(author)

    if author not in ignored_authors:
        for category in categories:
            outdir = os.path.join(output_directory, author, category)
            os.makedirs(outdir)
            for post in good[category]:
                src = os.path.join(author_dir, category, post)
                dst = os.path.join(outdir, post)
                os.symlink(src, dst)
print(f'dropped {len(ignored_authors)} authors ({len(authors) - len(ignored_authors)} remaining)') 
