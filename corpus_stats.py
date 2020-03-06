#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import os
import argparse
from collections import defaultdict
import json

parser = argparse.ArgumentParser(description="")
parser.add_argument('-i', '--input-directory', required=True, help='Input directory')
parser.add_argument('--single', action="store_true", help='')
args = parser.parse_args()

if args.single:
    data = {}
    authordata = defaultdict(list)
    files = sorted(os.listdir(args.input_directory))
    data['authors'] = len(files)
    for author_file in files:
        author = os.path.splitext(author_file)[0]
        with open(os.path.join(args.input_directory, author_file)) as i_f:
            for line in i_f:
                msg = json.loads(line)
                authordata[author].append(len(msg['body']))
    data['avg_docs'] = 0
    data['avg_doc_length'] = 0
    for author,msg_lengths in authordata.items():
        data['avg_docs'] += len(msg_lengths)
        data['avg_doc_length'] += sum(msg_lengths) / len(msg_lengths)
    data['avg_docs'] /= len(files)
    data['avg_doc_length'] /= len(files)
    print(data)
    exit()






data = {}
authors = sorted(os.listdir(args.input_directory))
data['authors'] = len(authors)
topicdata = defaultdict(list)
topicsizedata = defaultdict(list)
for author in authors:
    author_dir = os.path.join(args.input_directory, author)
    topics = sorted(os.listdir(author_dir))
    for topic in topics:
        topic_dir = os.path.join(author_dir, topic)
        documents = sorted(os.listdir(topic_dir))
        documents = [x for x in documents if '.json' in x]
        topicdata[topic].append(len(documents))
        doclengths = 0
        for document in documents:
            fullpath = os.path.join(topic_dir, document)
            with open(fullpath) as i_f:
                doclength= len(json.loads(i_f.read())['body'])
                doclengths += doclength
        avg_doc_length = doclengths/len(documents)
        topicsizedata[topic].append(avg_doc_length)

avg_docs = 0
for documents in topicdata.values():
    avg_docs += sum(documents) / len(documents)
data['avg_docs'] = avg_docs / len(topicdata)

avg_docs_size = 0
for documents in topicsizedata.values(): 
    avg_docs_size += sum(documents) / len(documents)
data['avg_doc_length'] = avg_docs_size / len(topicsizedata)

print(data)
