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

from bots import botlist
from bs4 import BeautifulSoup as BS
from collections import defaultdict
from langdetect import detect_langs

import argparse
import glob
import hoep as h
import json
import logging
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
import re
import string
import time
import tqdm

logging.basicConfig(level='INFO', format='%(asctime)s %(levelname)s: %(message)s')

parser = argparse.ArgumentParser(description="Reads and filters Reddit comment JSON files.")
parser.add_argument('-i', '--input-pattern', required=True, help='glob pattern containing the RC_20XX files')
parser.add_argument('-o', '--output-directory', required=True, help='Output directory')
parser.add_argument('-j', '--jobs', type=int, default=1, help='number of processes to use')
parser.add_argument('-p', '--lang-detect-probability', default=0.99, type=float, help='Minimum probability of language detection to keep a comment')
parser.add_argument('-ct','--post-characters-threshold', type=int, default=1000, help='Minimum post length (characters)')
parser.add_argument('-wt','--post-words-threshold', type=int, default=50, help='Minimum post length (words)')
parser.add_argument('-vt', '--post-vocabulary-threshold', type=int, default=20, help='minimum vocabulary richness')
args = parser.parse_args()

logging.info('reading file list')
files = sorted(glob.glob(args.input_pattern), reverse=False)
logging.info(f'read {len(files)} files, starting work')

def remove_markdown(text):
    html = h.render(text)
    return BS(html, features='html5lib').get_text()

def remove_citations(text):
    lines = text.split('\n')
    return '\n'.join([x for x in lines if not x.startswith('>')])

def remove_urls(text): 
    return re.sub('\S*https?:\/\/\S+', '<URL>', text, flags=re.MULTILINE)

def clean(text):
    text = remove_markdown(text)
    text = remove_citations(text)
    text = remove_urls(text)
    return text

def not_enough_characters(msg): 
    return len(msg['body_clean']) < args.post_characters_threshold

def not_enough_words(msg):
    return len(msg['words']) < args.post_words_threshold

def not_enough_different_words(msg):
    return len(set(msg['words'])) < args.post_vocabulary_threshold

def is_bot(msg): 
    return msg['author'] in botlist

def analyze(msg): 
    reasons = set()
    if not_enough_characters(msg):
        reasons.add('not enough characters')
    if not_enough_words(msg):
        reasons.add('not enough words')
    if not_enough_different_words(msg):
        reasons.add('not enough different words')
    if is_bot(msg): 
        reasons.add('is a bot')
    return reasons

def work(filename):
    if not os.path.isdir(args.output_directory):
        os.mkdir(args.output_directory)
    output_filename = os.path.join(args.output_directory, os.path.basename(filename))
    with open(filename) as i_f, open(output_filename, 'w') as o_f:
        reasons = defaultdict(int)
        for line in i_f:
            try: 
                msg = json.loads(line)
                msg['body_clean']= clean(msg['body'])
                msg['words'] = msg['body_clean'].translate(str.maketrans('', '', string.punctuation)).split()
                msg_reasons = analyze(msg)
                if not msg_reasons:
                    language_scores = sorted(list(detect_langs(msg['body'])), key=lambda x: x.prob, reverse=True)
                    scores = [x.prob for x in language_scores]
                    langs = [x.lang for x in language_scores]
                    if scores[0] > args.lang_detect_probability:
                        payload = {
                            'language': langs[0],
                            'subreddit': msg['subreddit'],
                            'author': msg['author'],
                            'body': msg['body_clean']
                        }
                        o_f.write(json.dumps(payload) + '\n')
                        msg_reasons.add('success')
                    else:
                        msg_reasons.add('language unclear')
                for r in msg_reasons:
                    reasons[r] += 1
            except Exception: 
                continue

        d = dict(reasons)
        d['file'] = filename
        with open(output_filename + '.stats', 'w') as o_f:
            json.dump(d, o_f)

with mp.Pool(processes=args.jobs) as pool:
    r = list(tqdm.tqdm(pool.imap(work, files), total=len(files)))
    print('done')
