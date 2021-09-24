import json
import pickle
import shutil
import warnings
from glob import glob

import click
import os
import logging
import pandas as pd
from tqdm import tqdm

from dbispipeline.pipeline.filename import FileReader, PathTransformer, FileWriter
from sklearn.pipeline import make_pipeline

logger = logging.getLogger(__name__)

FEATURE_TEXT = 'text_raw'
FEATURE_STANZA = 'stanza'
FEATURE_CONSTITUENTS = 'constituencies'
LANGUAGE_COLUMN = 'language'
DATASET_CSV = 'dataset.csv'

CORENLP_MODELS = '/home/benjamin/lib/stanford/models'
CORENLP_PROPERTIES = 'resources/stanford_properties'
CORENLP_PORT = 9000





@click.command(help='cleans accidental duplicated index columns')
@click.argument('input-file')
def clean_df_indices(input_file):
    df = pd.read_csv(input_file)
    index_columns = [c for c in df.columns if c.startswith('Unnamed: 0')]
    df = df.drop(columns=index_columns)
    df.to_csv(input_file, index=False)


@click.command()
@click.argument('input-directory')
def reattach_stanza_columns(input_directory):
    for language, path in zip(
        ['de', 'es'],
        ['data/processed/reddit/R-CL1-DE-TRANSLATION',
         'data/processed/reddit/R-CL2-ES']):
        stanzas = glob(path + '/stanza/*')
        names = [
            os.path.join(path, 'text_raw',
                         os.path.splitext(os.path.basename(x))[0] + '.txt')
            for x in stanzas]
        for const, raw in list(zip(stanzas, names)):
            if not os.path.isfile(raw):
                print('NOPE: %s' % raw)
        df = pd.DataFrame(dict(text_raw=names, stanza=stanzas))
        main_df = pd.read_csv(path + '/dataset.csv')
        combination = main_df.merge(df, on='text_raw')
        if combination.isnull().sum().sum() == 0:
            combination.to_csv(path + '/dataset.csv', index=False)
