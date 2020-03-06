#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import argparse
import tqdm
import json
import numpy as np
from collections import defaultdict

parser = argparse.ArgumentParser(description="")
parser.add_argument('-i', '--input-pattern', required=True, help='Input pattern')
args = parser.parse_args()

input_stat_files = sorted(glob.glob(args.input_pattern))

all_authors = defaultdict(int)
subreddits = defaultdict(int)
document_count = 0
document_lengths = []

for f in tqdm.tqdm(input_stat_files): 
    with open(f) as input_fh:
        js = json.load(input_fh)
        document_count += js['no_documents']

        for author, count in js['authors'].items():
            all_authors[author] += count

        for sub, count in js['subreddits'].items(): 
            subreddits[sub] += count

        document_lengths += js['doc_lengths']

print(f'COMMENTS:     {document_count:12d}')
print(f'AUTHORS:      {len(all_authors):12d}')
print(f'SUBREDDITS:   {len(subreddits):12d}')
print(f'AVG DOC/AUTH: {document_count/len(all_authors):12.3f}')
print(f'AVG DOCSIZE:  {np.mean(np.array(document_lengths)):12.0f}')

