# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes actors/actresses as nodes and common movies as weighted
edges. Different 20-year periods are used to set up separate networks and basic
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on various 20-year
windows.

@author: Hakan Hekimgil
"""

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
print("Reading titles...")
numtitles = 0
titleset = set()
peopleset = set()

# some helper functions
def titleinfo(n):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM titles WHERE tconst = ?;", (int(n),))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def findtitle(txt):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM titles WHERE primaryTitle = ?;", (txt,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def peopleinfo(n):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM people WHERE nconst = ?;", (int(n),))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def findperson(txt):
    conntemp = sqlite3.connect("data/imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM people WHERE primaryName = ?;", (txt,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "weights", "density", 
                               "max deg.", "max deg. (w)", "largest CC"])
ccdist = []

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
actids = db.fetchall()
assert len(actids) == 2

for year1 in range(1930,2010,10):
    year2 = year1 + 19
    G.clear()
    
    # read people and titles for a 20-year window
    db.execute(
            "SELECT DISTINCT t.tconst, nconst " + 
            "FROM (SELECT * FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "INNER JOIN (SELECT * FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            "ON t.tconst = p.tconst " + 
            "ORDER BY t.tconst;", (actids[0][0], actids[1][0], year1, year2))
    edgeinfo = pd.DataFrame(db.fetchall(), columns=["tconst", "nconst"])
    peopleset = set(edgeinfo.nconst.unique())
    G.add_nodes_from(edgeinfo.nconst.unique())
    prevedge = None
    tempset = set()
    for row in edgeinfo.itertuples(index=False):
        titleid = row.tconst
        actid = row.nconst
        #print(titleid, actid)
        if titleid != prevedge:
            prevedge = titleid
            tempset = set([actid])
        else:
            for node in tempset:
                if G.has_edge(node, actid):
                    G[node][actid]["weight"] += 1
                else:
                    G.add_edge(node, actid, weight=1)
            tempset.add(actid)
    
    #print("Number of nodes: ", G.number_of_nodes())
    #print("Number of edges: ", G.number_of_edges())
    print(nx.info(G))
    if G.number_of_nodes() <= 20:
        print("Nodes: ", G.nodes())
        for node in G.nodes():
            print(G.node[node])
    if G.number_of_edges() <= 20:
        print("Edges: ", G.edges())
    print("Network density:", nx.density(G))
    maxd = max([d for n,d in G.degree()])
    d50th = sorted([d for n,d in G.degree()])[-50]
    print("Maximum degree (unweighted):", maxd)
    maxdw = max([d for n,d in G.degree(weight="weight")])
    d50thw = sorted([d for n,d in G.degree(weight="weight")])[-50]
    print("Maximum degree (weighted):", maxdw)
    maxdact = [n for n,d in G.degree() if d==maxd]
    print("Actors/actresses with maximum degree:", [peopleinfo(m) for m in maxdact])
    print("Maximum degree:", max([d for n,d in G.degree()]))
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    print("Size of largest connected component:", maxconnectedsize)
    infodfrow = pd.Series({"period":str(year1)+"-"+str(year2), 
                           "# vertices":G.number_of_nodes(), 
                           "# edges":G.number_of_edges(), 
                           "weights":G.size(weight="weight"), 
                           "density":nx.density(G), 
                           "max deg.":maxd, 
                           "max deg. (w)":maxdw, 
                           "largest CC":maxconnectedsize}, 
        name=str(year1)+"-"+str(year2))
    infodf = infodf.append(infodfrow)
    temp = set([len(c) for c in connecteds])
    temp2 = [len(c) for c in connecteds]
    connectedsizes = {k:temp2.count(k) for k in temp}
    ccdist.append((str(year1)+"-"+str(year2),connectedsizes))
    print("Number of connected components according to size:", connectedsizes)
    
    #G.clear()

conn.close()
print("\n\nActors/actresses as vertices and movies as weighted (by number) edges:")
print("----------------------------------------------------------------------")
print(infodf)
#print(ccdist)
