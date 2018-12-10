# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes movies as nodes and common actors/actresses as weighted
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
    maxdmov = [n for n,d in G.degree() if d==maxd]
    print("Movies with maximum degree (unweighted):", [titleinfo(m) for m in maxdmov])
    maxdmovw = [n for n,d in G.degree(weight="weight") if d==maxdw]
    print("Movies with maximum degree (weighted):", [titleinfo(m) for m in maxdmovw])
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    #connecteds = nx.connected_components(G)
    #largestconnected = max(connecteds, key=len)
    #maxconnectedsize = len(largestconnected)
    #diameter = nx.diameter(G.subgraph(largestconnected))
    print("Size of largest connected component:", maxconnectedsize)
    #print("Diameter of largest connected component:", diameter)
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
print("\n\nMovies as vertices and actors/actresses as weighted (by number) edges:")
print("----------------------------------------------------------------------")
print(infodf)
#print(ccdist)
