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
from tqdm import tqdm, trange
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
        '--c-offset', type=int, required=False, default=500,
        help='Upper boundary offset to lower C value [500]')
parser.add_argument(
        '--c-step-size', type=int, required=False, default=500,
        help='C offset of one step [0]')
parser.add_argument(
        '--steps', type=int, required=False, default=1,
        help='How many steps should be genererated [1]')


args = parser.parse_args()
authors = sorted(os.listdir(args.input_directory))
ignored_authors = set()

if args.steps > 1 and args.c_step_size is None:
    raise ValueError(f'please provide a c_step_size for more than 1 steps')

if args.steps > 1 and args.c_step_size is not None and args.c_offset > args.c_step_size:
    raise ValueError(f'your steps size ({args.c_step_size}) cant be larger '
                     f'than your c_offset ({args.c_offset}), or your corpora '
                     f'will have duplicate posts')

post_map = defaultdict(dict)

for step in trange(args.steps, desc='steps'):
    c = args.c + step * args.c_step_size
    c_max = None
    if args.c_offset is not None:
        c_max = c + args.c_offset

    output_name = f'{args.m}_{c}'
    if c_max is not None:
        output_name += f'_{c_max}'
    indir = pathlib.Path(args.input_directory)
    output_directory = os.path.join(indir.parent, output_name)

    for author in tqdm(authors, desc='authors', leave=False):
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
                    if c_max is not None and size > c_max: 
                        bad[category].append(post)
                    elif size < c:
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

                    src_dir_abs = os.path.join(author_dir, category)
                    src_dir_rel = os.path.relpath(src_dir_abs, outdir)

                    src_file = os.path.join(src_dir_rel, post)
                    out_file = os.path.join(outdir, post)

                    os.symlink(src_file, out_file)
