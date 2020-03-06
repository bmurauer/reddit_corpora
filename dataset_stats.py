#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
import glob
import numpy as np
import multiprocessing as mp
from collections import defaultdict

import tqdm 

parser = argparse.ArgumentParser(description="")
parser.add_argument('-i', '--input-directory', required=True, help='Input directory')
parser.add_argument('-o', '--output-directory', required=True, help='Output directory')
parser.add_argument('-j', '--jobs', type=int, default=10, help='number of concurrent jobs')
args = parser.parse_args()

if not os.path.isdir(args.output_directory): 
    os.makedirs(args.output_directory)

def process_split_directory(directory):
    files = sorted(glob.glob(os.path.join(directory, 'RC_*')))
    output_file = os.path.join(args.output_directory, f'{os.path.basename(directory)}.stats')

    no_documents = 0
    authors = defaultdict(int)
    languages = defaultdict(int)
    subreddits = defaultdict(int)
    doc_lengths = []

    for f in files:
        with open(f) as input_fh:
            for line in input_fh:
                try: 
                    js = json.loads(line)
                    authors[js['author']] += 1
                    languages[js['language']] += 1
                    subreddits[js['subreddit']] += 1
                    doc_lengths.append(len(js['body_clean']))
                    no_documents += 1
                    del js
                except Exception:
                    pass

    with open(output_file, 'w') as output_fh:
        payload = {
            'no_documents': no_documents,
            'authors': authors,
            'subreddits': subreddits,
            'languages': languages,
            'doc_lengths': doc_lengths,
        }
        json.dump(payload, output_fh)

all_files = sorted(glob.glob(f'{args.input_directory}/RC_*'))
all_directories = [x for x in all_files if os.path.isdir(x)]

print(f'processing {len(all_directories)} directories with {args.jobs} jobs.')


with mp.Pool(processes=args.jobs) as pool:
    r = list(tqdm.tqdm(pool.imap(process_split_directory, all_directories), total=len(all_directories)))
    print('done')
