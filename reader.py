# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 02:46:33 2018

@author: hekim
"""

import pandas as pd

import shelve
# for storing, retrieving Python objects on disk

folder = "data/"
ratingsfile = "title.ratings.tsv"
titlesfile = "title.basics.tsv"
namesfile = "name.basics.tsv"
ratingsdata = pd.read_table(folder+ratingsfile, sep="\t")
titlesdata = pd.read_table(folder+titlesfile, sep="\t")
namesdata = pd.read_table(folder+namesfile, sep="\t")

#ratingsdata.head()
#titlesdata.head()
#namesdata.head()

#ratingsdata[0,"numVotes"]
#titlesdata.shape

professions = {}
genres = {}


ddb = shelve.open(path.join(cachedir,"tfd" + str(nf) + ".hhh"), flag="n")
ddb["termfreq"+str(i)] = termfreq[i]
ddb.close()
qdb["postingslist"] = temp
qdb["collectionfreq2"] = collectionfreq2
tfreq = ddb["termfreq"+str(docid)]
