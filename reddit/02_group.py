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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from collections import defaultdict
from langdetect import detect_langs
from multiprocessing import JoinableQueue
from tqdm import tqdm

import argparse
import json
import pickle
import os
import glob
import numpy as np
import logging
logging.basicConfig(level='INFO', format='%(asctime)s %(levelname)s: %(message)s')
import tempfile
import datetime


def find_overlap(index, limits):
    target_field = index['target_field']
    grouping_field = index['grouping_field']

    # e.g., all languages in the index
    all_groups = set(index['data'].keys())
    if not limits[grouping_field]:
        logging.warning(f'you are grouping by {grouping_field}, but you have not'
        ' restricted this field. There will probably be no overlap and you will'
        ' probably get an empty result.')
        groups = all_groups
    elif limits[grouping_field] - all_groups:
        logging.error(f'the requested grouping field values {limits[grouping_field] -all_groups} are not in the index')
        exit()
    else:
        groups = all_groups.intersection(limits[grouping_field])


    # now is, e.g., [de, en]
    logging.debug(f'limiting groups to {all_groups}')
    result = defaultdict(dict)
    
    # now look for targets that are common in all found groups
    targets = set()
    for group in groups:
        if not targets:
            targets = set(index['data'][group])
        else:
            targets = targets.intersection(set(index['data'][group]))

    logging.debug(f'limiting targets to {targets}')
    return {
        'group': groups,
        'target': targets
    }

def find_posts(target_field, grouping_field, filenames, limits, c):
    result = defaultdict(lambda: defaultdict(list))
    for filename in tqdm(filenames):
        with open(filename) as i_f:
            for line in i_f:
                data = json.loads(line)
                if len(data['body']) < c:
                    continue
                adding = True
                for key in limits.keys():
                    if not limits[key]:
                        continue
                    if data[key] not in limits[key]:
                        adding = False
                        break
                if adding:
                    post_target = data[target_field]
                    post_group = data[grouping_field]
                    result[post_target][post_group].append(data)
    return result


def filter_min_posts(data, m):
    for key in data.keys():
        data[key] = {k: v for k, v in data[key].items() if len(v) >= m}
    return data

def filter_desired_groups(data, desired_groups):
    return {target:groups for target, groups in data.items() if set(groups.keys()) == set(desired_groups)}

def check_output_dir(directory):
    if not os.path.isdir(directory):
        os.mkdir(directory)
        logging.info('storing output to %s' % directory)
        return directory
    else:
        tmp = tempfile.mkdtemp()
        logging.warning(f'The output directory {directory} is not empty. Storing to temporary directory {tmp} instead.')
        return tmp

def create_non_border_corpus(target_field, limits, input_files, c, m):
    values = defaultdict(list)
    valids = set()
    timestamp = datetime.datetime.now().strftime('%d-%m-%Y--%H-%M')
    output_directory = check_output_dir(f'{target_field}_{timestamp}')
    logging.info(limits)
    authors = limits['author']
    languages = limits['language']
    subreddits = limits['subreddit']
    for filename in tqdm(files):
        with open(filename) as i_f:
            for line in i_f:
                msg = json.loads(line)
                if len(msg['body']) < c:
                    continue
                if authors and msg['author'] not in authors:
                    continue
                if languages and msg['language'] not in languages:
                    continue
                if subreddits and msg['subreddit'] not in subreddits:
                    continue
                key = msg[target_field]
                values[key].append(msg)
                if len(values[key]) == m:
                    flush(key, values[key], output_directory)
                    valids.add(key)
                    values[key] = []

    # flush remaining buffers
    if not valids:
        logging.warning('No results left to store, exiting')
    for key in valids:
        flush(key, values[key], output_directory)

def flush(key, values, output_directory):
    outfile = os.path.join(output_directory, key + '.json')
    with open(outfile, 'a') as o_f:
        for v in values:
            o_f.write(json.dumps(v) + '\n')

def create_cross_border_corpus(target_field, grouping_field, limits, input_files, c, m, index_location):
    index = get_index(target_field, grouping_field, input_files, limits, index_location)

    overlap = find_overlap(index, limits)
    limits[target_field] = overlap['target']
    limits[grouping_field] = overlap['group']

    logging.info(f'collecting posts from {len(overlap["target"])} {target_field}s')

    posts = find_posts(target_field, grouping_field, input_files, limits, c)
    if not posts:
        logging.warning('no posts left! exiting...')
        exit()

    logging.info(f'filtering {grouping_field}s with not enough posts')
    posts = filter_min_posts(posts, m)
    if not posts:
        logging.warning('no posts left! exiting...')
        exit()

    logging.info(f'filtering {target_field}s with not enough {grouping_field}s')
    posts = filter_desired_groups(posts, overlap['group'])
    if not posts:
        logging.warning('no posts left! exiting...')
        exit()
 
    limits_string = []
    if limits['subreddit']:
        limits_string += limits['subreddit']
    if limits['language']:
        limits_string += limits['language']
    if limits_string: 
        limits_string = '_'.join(limits_string)
    timestamp = datetime.datetime.now().strftime('%d-%m-%Y--%H-%M') 
    outdir = check_output_dir(f'{target_field}_{grouping_field}_{limits_string}_{m}_{c}_{timestamp}')
    store_result(posts, outdir)

def get_index(target_field, grouping_field, input_files, limits, index_location):
    index_filename = os.path.join(index_location, f'{target_field}_{grouping_field}.json')
    if os.path.isfile(index_filename):
        try:
            with open(index_filename) as i_f:
                index = json.load(i_f)
                if 'target_field' in index and 'grouping_field' in index:
                    stored_target, stored_grouping = index['target_field'], index['grouping_field']
                else:
                    raise Exception('index probably old')
 
                if target_field != stored_target or grouping_field != stored_grouping:
                    logging.warning('incompatible index file given.')
                    logging.warning(f'stored fields:  {stored_target}, {stored_grouping}')
                    logging.warning(f'reqired fields: {target_field}, {grouping_field}' )
                    logging.warning('calculating new index')
                    return calculate_new_index(target_field, grouping_field, input_files, index_location)
                else:
                    logging.info(f'using index: {index_filename}')
                    return index
        except Exception as e:
            logging.warning(f'exception during parsing index file {index_filename}: {e}')
            logging.warning('calculating new index')
            return calculate_new_index(target_field, grouping_field, input_files, index_location)
    else:
        logging.warning(f'could not find filename {index_filename}, calculating new index')
        return calculate_new_index(target_field, grouping_field, input_files, index_location)

        
def calculate_new_index(target_field, grouping_field, files, index_location):
    index_filename = os.path.join(index_location, f'{target_field}_{grouping_field}.json')
    values = defaultdict(set)
    unique_values = set()
    logging.info('starting work')
    for filename in tqdm(files):
        with open(filename) as i_f:
            for line in i_f:
                msg = json.loads(line)
                target = msg[target_field]
                group = msg[grouping_field]
                values[group].add(target)
                unique_values.add(target)
    data = {k: list(v) for k,v in dict(values).items()}
    payload = {
        'target_field': target_field,
        'grouping_field': grouping_field,
        'data': data
    }
    store_index(payload, index_filename)
    return payload

def store_index(index, index_filename):
    logging.info('storing index to %s' % index_filename)
    with open(index_filename, 'w') as o_f:
        json.dump(index, o_f)

def store_result(data, output_dir):
    for i, author in enumerate(list(data.keys())):
        author_dir = os.path.join(output_dir, author)
        if not os.path.isdir(author_dir):
            os.mkdir(author_dir)
        for lang in data[author].keys():
            lang_dir = os.path.join(author_dir, lang)
            if not os.path.isdir(lang_dir):
                os.mkdir(lang_dir)
            for j, post in enumerate(data[author][lang]):
                file_name = os.path.join(lang_dir, '%06d.json' % j)
                with open(file_name, 'w') as o_f:
                    o_f.write(json.dumps(post))


parser = argparse.ArgumentParser(description="Groups Reddit comments by specified parameters")
parser.add_argument('-i', '--input-pattern', required=True, help='glob pattern for input files')
parser.add_argument('-t', '--target-field', choices=['subreddit', 'author', 'language'], required=True, help='which field should be used for a target')
parser.add_argument('-g', '--grouping-field', choices=['subreddit', 'author', 'language'], required=False, help='which field should be used for grouping the comments?')
parser.add_argument('-a', '--authors', default=None, help='which authors to use')
parser.add_argument('-l', '--languages', default=None, help='which languages to use')
parser.add_argument('-s', '--subreddits', default=None, help='which subreddits to use')
parser.add_argument('-m', default=1, type=int, help='How many documents must be present in each group per target')
parser.add_argument('-c', default=1000, type=int, help='min. length of remaining documents')
parser.add_argument('-idx', '--index-directory-location', default=os.path.expanduser('~'), help='Where to store intermediate index files')

args = parser.parse_args()

files = sorted(glob.glob(args.input_pattern))

authors = None
if args.authors:
    authors = set(args.authors.split(','))
languages = None
if args.languages:
    languages = set(args.languages.split(','))
subreddits = None
if args.subreddits:
    subreddits = set(args.subreddits.split(','))

limits = {
    'author': authors,
    'language': languages,
    'subreddit': subreddits
}

if args.target_field == args.grouping_field:
    logging.error('please provide different fields for target and grouping')
    exit()

if args.grouping_field:
    logging.info(f'grouping by {args.grouping_field} for every {args.target_field}')
    create_cross_border_corpus(args.target_field, args.grouping_field, limits, files, args.c, args.m, args.index_directory_location)
else:
    logging.info('not grouping')
    create_non_border_corpus(args.target_field, limits, files, args.c, args.m)
