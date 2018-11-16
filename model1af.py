# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes actors/actresses as nodes and common movies as weighted
edges. Different 20-year periods are used to set up separate networks and basic
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on various 20-year
windows.

VERSION A: PROCESSES THE WHOLE MOVIE DATABASE INSTEAD OF 20-YEAR WINDOWS.
VERSION F: FILTERS FOR MOVIES ACCORDING TO RATINGS.
           (numVotes >= 200/300; averRating >= 6.0/7.0)

@author: Hakan Hekimgil
"""

arating = 7.0
nvotes = 300

import numpy as np
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

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "density", "max degree", "largest CC"])
ccdist = []
top50df = pd.DataFrame(columns=["degree", "name", "birth", "death"])

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(1930,2010,100):
    year2 = year1 + 99
    G.clear()
    
    # read people and titles for a 20-year window
#    db.execute(
#            "SELECT DISTINCT t.tconst, nconst " + 
#            "FROM (SELECT nconst, tconst FROM principals WHERE catid = ? OR catid = ?) AS p " + 
#            "INNER JOIN (SELECT tconst FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
#            "ON t.tconst = p.tconst " + 
#            "ORDER BY t.tconst;", (catids[0][0], catids[1][0], year1, year2))
    db.execute(
            "SELECT DISTINCT tr.tconst, nconst FROM (" + 
            " SELECT t.tconst FROM (" + 
            "  SELECT tconst FROM ratings WHERE numVotes >= ? AND averageRating >= ?) AS r " + 
            " INNER JOIN (" + 
            "  SELECT tconst FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            " ON t.tconst = r.tconst) AS tr " + 
            "INNER JOIN (SELECT nconst, tconst FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "ON tr.tconst = p.tconst " + 
            "ORDER BY tr.tconst;", (nvotes, arating, year1, year2, catids[0][0], catids[1][0]))
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
    print("Maximum degree:", maxd)
    top50dact = sorted([(d,n) for n,d in G.degree() if d >= d50th], reverse=True)
    top50dactfull = [(d,peopleinfo(n)) for d,n in top50dact]
    ind=0
    for t in top50dactfull:
        ind += 1
        death = np.NAN
        if t[1][3] != "NULL":
            death = int(t[1][3])
        birth = np.NAN
        if t[1][2] != "NULL":
            birth = int(t[1][2])
        top50dfrow = pd.Series({"degree":int(t[0]), 
                                "name":t[1][1], 
                                "birth":birth,
                                "death":death}, name=ind)
        top50df = top50df.append(top50dfrow)
    #print("Actors/actresses with maximum degrees:", [peopleinfo(m) for m in top20dact])
    print("Maximum degree:", max([d for n,d in G.degree()]))
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    print("Size of largest connected component:", maxconnectedsize)
    infodfrow = pd.Series({"period":str(year1)+"-"+str(year2), 
                           "# vertices":G.number_of_nodes(), 
                           "# edges":G.number_of_edges(), 
                           "density":nx.density(G), 
                           "max degree":max([d for n,d in G.degree()]), 
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
print("\n\nActors/actresses with maximum degrees:")
print("--------------------------------------")
print(top50df)
#print(ccdist)
