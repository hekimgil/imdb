# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes movies as nodes and common actors/actresses as weighted
edges. Separate networks are observed on a yearly basis and basic network
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on a yearly
basis.

@author: Hakan Hekimgil
"""

# subgraph partition parameters
year1 = 2010
year2 = 2018


import pandas as pd
import sqlite3
import networkx as nx

# initialize graph
G = nx.Graph()

# connect to database
conn = sqlite3.connect("data/imdblite.db")
db = conn.cursor()

# start reading and moving to db
# transfer titles
print("Readin titles...")
numtitles = 0
titleset = set()
peopleset = set()

# some helper functions
def titleinfo(n):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM titles WHERE tconst = ?;", (n,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def peopleinfo(n):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM people WHERE nconst = ?;", (n,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "max degree", "largest CC"])
ccdist = []

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
actids = db.fetchall()
assert len(actids) == 2

for year1 in range(1930,2010,10):
    year2 = year1 + 19
    # read and add titles
    db.execute("SELECT * FROM titles WHERE startYear >= ? AND startYear <= ?;", (year1, year2))
    movies = pd.DataFrame(db.fetchall(), columns=["tconst", "primaryTitle", "startYear", "endYear", "runtimeMinutes", "genre1", "genre2","genre3"])
    rows, columns = movies.shape
    print("{:,} titles found...".format(rows))
    for row in movies.itertuples(index=False):
        titleid = row.tconst
        titlename = row.primaryTitle
        #print(titleid, titlename)
        titleset.add(titleid)
        G.add_node(titleid, title=titlename)
        numtitles += 1
    print("{:,} titles added as nodes...".format(numtitles))
    
    db.execute(
            "SELECT DISTINCT t.tconst, nconst " + 
            "FROM (SELECT * FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            "INNER JOIN (SELECT * FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "ON t.tconst = p.tconst " + 
            "ORDER BY nconst;", (year1, year2, actids[0][0], actids[1][0]))
    edgeinfo = pd.DataFrame(db.fetchall(), columns=["tconst", "nconst"])
    prevedge = None
    tempset = set()
    for row in edgeinfo.itertuples(index=False):
        titleid = row.tconst
        actid = row.nconst
        #print(titleid, actid)
        if actid != prevedge:
            prevedge = actid
            tempset = set([titleid])
        else:
            for node in tempset:
                if G.has_edge(node, titleid):
                    G[node][titleid]["weight"] += 1
                else:
                    G.add_edge(node, titleid, weight=1)
            tempset.add(titleid)
    
    rows, columns = movies.shape
    
    print("Number of nodes: ", G.number_of_nodes())
    print("Number of edges: ", G.number_of_edges())
    if G.number_of_nodes() <= 20:
        print("Nodes: ", G.nodes())
        for node in G.nodes():
            print(G.node[node])
    if G.number_of_edges() <= 20:
        print("Edges: ", G.edges())
    print("Maximum degree:", max([d for n,d in G.degree()]))
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    print("Size of largest connected component:", maxconnectedsize)
    infodfrow = pd.Series({"period":str(year1)+"-"+str(year2), 
                           "# vertices":G.number_of_nodes(), 
                           "# edges":G.number_of_edges(), 
                           "max degree":max([d for n,d in G.degree()]), 
                           "largest CC":maxconnectedsize}, 
        name=str(year1)+"-"+str(year2))
    infodf = infodf.append(infodfrow)
    temp = set([len(c) for c in connecteds])
    temp2 = [len(c) for c in connecteds]
    connectedsizes = {k:temp2.count(k) for k in temp}
    ccdist.append((str(year1)+"-"+str(year2),connectedsizes))
    print("Number of connected components according to size:", connectedsizes)
    
    G.clear()

conn.close()
print("\n\nMovies as vertices and actors/actresses as weighted (by number) edges:")
print("-------------------------------------------------------------------------")
print(infodf)
#print(ccdist)
