# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes movies as nodes and common actors/actresses as weighted
edges. Different 20-year periods are used to set up separate networks and basic
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on various 20-year
windows.

VERSION A: PROCESSES THE WHOLE MOVIE DATABASE INSTEAD OF 20-YEAR WINDOWS.
VERSION F: FILTERS FOR MOVIES ACCORDING TO RATINGS.
           (numVotes >= 200; averRating >= 6.0)
VERSION R: FILTERS MORE RECENT AND MORE POPULAR MOVIES
           (startYear > 2000; numVotes >= 300; averRating >= 7.0)

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
top50df = pd.DataFrame(columns=["degree", "name", "startYear"])

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(2000,2010,100):
    year2 = year1 + 99
    G.clear()
    
    db.execute(
            "SELECT DISTINCT tr.tconst, nconst FROM (" + 
            " SELECT t.tconst FROM (" + 
            "  SELECT tconst FROM ratings WHERE numVotes >= ? AND averageRating >= ?) AS r " + 
            " INNER JOIN (" + 
            "  SELECT tconst FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            " ON t.tconst = r.tconst) AS tr " + 
            "INNER JOIN (SELECT nconst, tconst FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "ON tr.tconst = p.tconst " + 
            "ORDER BY nconst;", (nvotes, arating, year1, year2, catids[0][0], catids[1][0]))
    edgeinfo = pd.DataFrame(db.fetchall(), columns=["tconst", "nconst"])
    titleset = set(edgeinfo.tconst.unique())
    print("{:,} nodes determined...".format(len(titleset)))
    G.add_nodes_from(edgeinfo.tconst.unique())
    print("{:,} nodes added to graph...".format(G.number_of_nodes()))
    prevedge = None
    tempset = set()
    temp = 0
    for row in edgeinfo.itertuples(index=False):
        temp += 1
        if temp % 250000 == 0:
            print("checking row {:,}".format(temp))
        titleid = row.tconst
        actid = row.nconst
        #print(titleid, actid)
        if actid != prevedge:
            prevedge = actid
            del(tempset)
            tempset = set([titleid])
        else:
            for node in tempset:
                if G.has_edge(node, titleid):
                    G[node][titleid]["weight"] += 1
                else:
                    G.add_edge(node, titleid, weight=1)
            tempset.add(titleid)
    del(edgeinfo)
    
#    rows, columns = movies.shape
    
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
    top50dmov = sorted([(d,n) for n,d in G.degree() if d >= d50th], reverse=True)
    top50dmovfull = [(d,titleinfo(n)) for d,n in top50dmov]
    ind=0
    for t in top50dmovfull:
        ind += 1
        year = np.NAN
        if t[1][2] != "NULL":
            year = int(t[1][2])
        top50dfrow = pd.Series({"degree":int(t[0]), 
                                "name":t[1][1], 
                                "startYear":year}, name=ind)
        top50df = top50df.append(top50dfrow)
    #print("Movies with maximum degree:", [titleinfo(m) for m in top20dmov])
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
                           "density":nx.density(G), 
                           "max degree":max([d for n,d in G.degree()]), 
                           "largest CC":maxconnectedsize}, 
        name=str(year1)+"-"+str(year2))
    infodf = infodf.append(infodfrow)
    temp = set([len(c) for c in connecteds])
    temp2 = [len(c) for c in connecteds]
    connectedsizes = {k:temp2.count(k) for k in temp}
    del(temp)
    del(temp2)
    ccdist.append((str(year1)+"-"+str(year2),connectedsizes))
    print("Number of connected components according to size:", connectedsizes)
    
    #G.clear()

conn.close()
print("\n\nMovies as vertices and actors/actresses as weighted (by number) edges:")
print("----------------------------------------------------------------------")
print(infodf)
print("\n\nMovies with maximum degrees:")
print("----------------------------")
print(top50df)
#print(ccdist)
