# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes actors/actresses as nodes and common movies as weighted
edges. Different 20-year periods are used to set up separate networks and basic
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on various 20-year
windows.

@author: Hakan Hekimgil, Pan
"""

arating = 5.0
nvotes = 300

import numpy as np
import pandas as pd
import sqlite3
import networkx as nx
import operator
import pickle
#%%
# initialize graph
G = nx.Graph()

# connect to database
conn = sqlite3.connect("imdblite.db")
db = conn.cursor()

# start reading and moving to db
# transfer titles
print("Reading titles...")
numtitles = 0
titleset = set()
peopleset = set()

# some helper functions
def titleinfo(n):
    conntemp = sqlite3.connect("imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM titles WHERE tconst = ?;", (int(n),))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def findtitle(txt):
    conntemp = sqlite3.connect("imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM titles WHERE primaryTitle = ?;", (txt,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def peopleinfo(n):
    conntemp = sqlite3.connect("imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM people WHERE nconst = ?;", (int(n),))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp
def findperson(txt):
    conntemp = sqlite3.connect("imdblite.db")
    dbtemp = conntemp.cursor()
    dbtemp.execute("SELECT * FROM people WHERE primaryName = ?;", (txt,))
    infotemp = dbtemp.fetchone()
    conntemp.close()
    return infotemp

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "density", "max degree", "largest CC"])
ccdist = []
top50df = pd.DataFrame(columns=["degree", "name", "birth", "death"])

#%%
# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(1960,1980,100):
    year2 = year1 + 20
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
    print("Initial graph")
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
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    print("Size of largest connected component:", maxconnectedsize)
    infodfrow = pd.Series({"period":str(year1)+"-"+str(year2), 
                           "# vertices":G.number_of_nodes(), 
                           "# edges":G.number_of_edges(), 
                           "density":nx.density(G), 
                           "max degree":max([d for n,d in G.degree()]), 
                           "largest CC":maxconnectedsize}, 
        name="Whole graph")
    infodf = infodf.append(infodfrow)
    temp = set([len(c) for c in connecteds])
    temp2 = [len(c) for c in connecteds]
    connectedsizes = {k:temp2.count(k) for k in temp}
    ccdist.append((str(year1)+"-"+str(year2),connectedsizes))
    print("Number of connected components according to size:", connectedsizes)
    
    # PICK THE LARGEST CONNECTED COMPONENT
    largestcomponents = [cc for cc in connecteds if len(cc) == maxconnectedsize]
    largestcc = largestcomponents[0]
    H = G.subgraph(largestcc).copy()
    G.clear()
    del(G)
    print(nx.info(H))
    print("Network density:", nx.density(H))
    infodfrow = pd.Series({"period":str(year1)+"-"+str(year2), 
                           "# vertices":H.number_of_nodes(), 
                           "# edges":H.number_of_edges(), 
                           "density":nx.density(H), 
                           "max degree":max([d for n,d in H.degree()]), 
                           "largest CC":maxconnectedsize}, 
        name="Largest component")
    infodf = infodf.append(infodfrow)
    top50dact = sorted([(d,n) for n,d in H.degree() if d >= d50th], reverse=True)
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


    #G.clear()

conn.close()
print("\n\nActors/actresses as vertices and movies as weighted (by number) edges:")
print("----------------------------------------------------------------------")
print(infodf)
print("\n\nActors/actresses with maximum degrees:")
print("--------------------------------------")
print(top50df)
#print(ccdist)

# add names
conntemp = sqlite3.connect("imdblite.db")
dbtemp = conntemp.cursor()
nameerror = 0
for node in H.nodes():
    dbtemp.execute("SELECT * FROM people WHERE nconst = ?;", (int(node),))
    infotemp = dbtemp.fetchone()
    if infotemp != None:
        H.node[int(node)]["name"] = infotemp[1]
    else:
        H.node[int(node)]["name"] = "---"
        nameerror += 1
conntemp.close()
if nameerror > 0:
    print("There were {:} name errors...".format(nameerror))

nx.write_graphml(H, "imdb2.graphml")
#%% Draw network
from matplotlib import pylab as pl
import matplotlib.pyplot as plt
plt.figure(figsize=(17,17))
node_size = [2*H.degree(v) for v in H]
pos = nx.spring_layout(H)
edge_width = [0.1*H[u][v]['weight'] for u,v in H.edges()]
nx.draw_networkx(H, pos=pos, alpha=0.6, node_size=node_size, width=edge_width, \
                 node_color='r', with_labels=False)
#%% Prediction based on different Measurements
#input name of target artist
name = 'Bruce Lee'
act_id = findperson(name)[0]

#%% Common Neighbors
non_neighbors = nx.non_neighbors(H, act_id)
common_neigh= [(n, len(list(nx.common_neighbors(H, act_id, n)))) for n in non_neighbors]
common_neigh.sort(key=lambda n: n[1], reverse=True)
print(common_neigh[0:10])
with open('common_neigh.pkl','wb') as f:
    pickle.dump(common_neigh, f)
#%% Jaccard Coefficient
L = nx.jaccard_coefficient(H)
jacc = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L if act_id in (n[0], n[1])]
jacc.sort(key=lambda n: n[1], reverse=True)
print(jacc[0:10])
with open('jacc.pkl','wb') as f:
    pickle.dump(jacc, f)
    
#%%  Resource Allocation
L2 = list(nx.resource_allocation_index(H))
resource_allo = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L2 if act_id in (n[0], n[1])]
resource_allo.sort(key=lambda n: n[1], reverse=True)
print(resource_allo[0:10])
with open('resource_allo.pkl','wb') as f:
    pickle.dump(resource_allo, f)

#%%  Adamic-Adar Index
L3 = list(nx.adamic_adar_index(H)) 
adamic_adar = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L3 if act_id in (n[0], n[1])]
adamic_adar.sort(key=lambda n: n[1], reverse=True)
print(resource_allo[0:10])
with open('adamic_adar.pkl','wb') as f:
    pickle.dump(adamic_adar, f)
    
#%% test reload pkl files
with open('common_neigh.pkl','rb') as f:
    listneigh = pickle.load(f)