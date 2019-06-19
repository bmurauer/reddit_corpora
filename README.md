# Generating Cross-Domain Text Classification Corpora from Social Media Comments

These scripts create cross-domain NLP corpora from reddit comments. 

To use them, you need a collection of reddit comments, e.g. from 
https://files.pushshift.io/reddit/comments/

 1. Filtering: `01_filter.py`
 2. Grouping: `02_group.py`

See all available options in the corresponding help menu entries (call the scripts with `-h`).
The directory `corpora` contains pre-calculated corpora with parameters described in the paper.
