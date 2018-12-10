# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model initially starts with actors/actresses as nodes and common movies as
weighted edges. Then the resulting graph is decomposed to obtain a graph where
movies are nodes and common actors/actresses are weighted edges.

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

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "weights", "density", 
                               "max deg.", "max deg. (w)", "largest CC"])
ccdist = []
top50df = pd.DataFrame(columns=["degree", "name", "birth", "death"])
top50dfw = pd.DataFrame(columns=["degree", "name", "birth", "death"])

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(2010,2020,100):
    year2 = year1 + 10
    G.clear()
    
    db.execute(
            "SELECT DISTINCT tr.tconst, nconst, primaryTitle FROM (" + 
            " SELECT t.tconst, primaryTitle FROM (" + 
            "  SELECT tconst FROM ratings WHERE numVotes >= ? AND averageRating >= ?) AS r " + 
            " INNER JOIN (" + 
            "  SELECT tconst, primaryTitle FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            " ON t.tconst = r.tconst) AS tr " + 
            "INNER JOIN (SELECT nconst, tconst FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "ON tr.tconst = p.tconst " + 
            "ORDER BY tr.tconst;", (nvotes, arating, year1, year2, catids[0][0], catids[1][0]))
    edgeinfo = pd.DataFrame(db.fetchall(), columns=["tconst", "nconst", "primaryTitle"])
    peopleset = set(edgeinfo.nconst.unique())
    G.add_nodes_from(edgeinfo.nconst.unique())
    prevedge = None
    tempset = set()
    for row in edgeinfo.itertuples(index=False):
        titleid = row.tconst
        actid = row.nconst
        titlename = row.primaryTitle
        #print(titleid, actid)
        if titleid != prevedge:
            prevedge = titleid
            tempset = set([actid])
        else:
            for node in tempset:
                if G.has_edge(node, actid):
                    G[node][actid]["weight"] += 1
                    G[node][actid]["movies"] = G[node][actid]["movies"][:-2] + '", "' + titlename + '"]'
                else:
#                    G.add_edge(node, actid, weight=1)
                    G.add_edge(node, actid, weight=1, movies='["' + titlename + '"]')
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
    print("Maximum degree (unweighted):", maxd)
    maxdw = max([d for n,d in G.degree(weight="weight")])
    d50thw = sorted([d for n,d in G.degree(weight="weight")])[-50]
    print("Maximum degree (weighted):", maxdw)
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
                           "weights":H.size(weight="weight"), 
                           "density":nx.density(H), 
                           "max deg.":max([d for n,d in H.degree()]), 
                           "max deg. (w)":max([d for n,d in H.degree(weight="weight")]), 
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
    
    top50dactw = sorted([(dw,n) for n,dw in H.degree(weight="weight") if dw >= d50thw], reverse=True)
    top50dactfullw = [(dw,peopleinfo(n)) for dw,n in top50dactw]
    indw=0
    for t in top50dactfullw:
        indw += 1
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
        top50dfw = top50dfw.append(top50dfrow)


    #G.clear()

conn.close()
print("\n\nActors/actresses as vertices and movies as weighted (by number) edges:")
print("----------------------------------------------------------------------")
print(infodf)
print("\n\nActors/actresses with maximum degrees (unweighted):")
print("---------------------------------------------------")
print(top50df)
print("\n\nActors/actresses with maximum degrees (weighted):")
print("-------------------------------------------------")
print(top50dfw)
#print(ccdist)

# add names
conntemp = sqlite3.connect("data/imdblite.db")
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

nx.write_graphml(H, "artists.graphml")







def weakenedge(G1, node1, node2):
    if G1.edges[node1, node2]["weight"] > 1:
        G1.edges[node1, node2]["weight"] -= 1
        return True
    else:
        G1.remove_edge(node1, node2)
    return False

def transfercluster(G1,G2,node):
    cluster = [n2 for (n1, n2) in G1.edges(node)]
    cluster.append(node)
    clusterlinkers = []
    while len(cluster) > 0:
        node1 = cluster.pop()
        for node2 in cluster:
            if weakenedge(G1, node1, node2):
                clusterlinkers.append(node2)
        if G1.degree(node1) == 0:
            G1.remove_node(node1)
    return clusterlinkers


# let's start decomposing the graph to form a new one




from itertools import combinations
# a copy of the previous graph to decompose
H0 = H.copy()
# the new graph to be formed
M = nx.Graph()
movieno = 0
linkers = dict()


# start by pruning the most certain branches
# (the ones where there is a node with clustering coef. 1 and its cluster has
# all edges of weight 1)
singles = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
updated = True
numfullyclustered = len(singles)
numnodesinitial = H0.number_of_nodes()
print("Initially, there are {:,} nodes that are fully clustered out of a total of {:,} nodes...".format(len(singles),H0.number_of_nodes()))
while len(singles) != 0 and updated:
    # find simplest clusters through fully clustered nodes
    updated = False
    while len(singles) != 0:
        node = singles.pop()
        
        cluster = [n2 for (n1, n2) in H0.edges(node)]
        cluster.append(node)
        singles = singles - set(cluster)
        
        clusteredges = list(combinations((cluster),2))
        weights = [H0.edges[e]["weight"] for e in clusteredges]
        if np.mean(weights) == 1:
            updated = True
            movieno += 1
            M.add_node(movieno, cast="[]")
            while len(cluster) > 0:
                node1 = cluster.pop()
                for node2 in cluster:
                    if weakenedge(H0, node1, node2):
                        if node2 in linkers:
                            linkers[node2].add(movieno)
                        else:
                            linkers[node2] = set([movieno])
                    else:
                        M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H.node[node2]["name"] + ", ]"
                M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H0.node[node1]["name"] + "]"
                if H0.degree(node1) == 0:
                    if node1 in linkers:
                        for linknode in linkers[node1]:
                            M.add_edge(linknode, movieno)
                    H0.remove_node(node1)
                    if node1 in singles:
                        singles.remove(node1)
                else:
                    if node1 in linkers:
                        linkers[node1].add(movieno)
                    else:
                        linkers[node1] = set([movieno])
                
#        dummy = transfercluster(H0,M,node)
    singles = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
numnodesafter1 = H0.number_of_nodes()
print("After the first clearance, {:,} nodes out of {:,} remain...".format(numnodesafter1,numnodesinitial))
print(nx.info(H0))
print(nx.info(M))
print()

nx.write_graphml(H0, "remaining1.graphml")
nx.write_graphml(M, "converted1.graphml")



# remove the two-node clusters
twonodes = [tuple(c) for c in nx.connected_components(H0) if len(c) == 2]
for (node1,node2) in twonodes:
    for i in range(H0.edges[node1, node2]["weight"],0,-1):
        movieno += 1
        M.add_node(movieno, cast="[" + H0.node[node1]["name"] + ", " + H0.node[node2]["name"] + "]")
        if i > 1:
            if node1 in linkers:
                linkers[node1].add(movieno)
            else:
                linkers[node1] = set([movieno])
            if node2 in linkers:
                linkers[node2].add(movieno)
            else:
                linkers[node2] = set([movieno])
    if node1 in linkers:
        for linknode in linkers[node1]:
            M.add_edge(linknode, movieno)
    H0.remove_node(node1)
    if node2 in linkers:
        for linknode in linkers[node2]:
            M.add_edge(linknode, movieno)
    H0.remove_node(node2)
numnodesafter2 = H0.number_of_nodes()
print("After the second clearance, {:,} nodes out of the previous {:,} remain...".format(numnodesafter2,numnodesafter1))
print(nx.info(H0))
print(nx.info(M))
print()

nx.write_graphml(H0, "remaining2.graphml")
nx.write_graphml(M, "converted2.graphml")



# continue by pruning other certain branches
# (the ones where there is a node with clustering coef. 1, with less than 4 edges 
# and total weighted degree less than 5)

singles = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
updated = True
while len(singles) != 0 and updated:
    # find simplest clusters through fully clustered nodes
    updated = False
    while len(singles) != 0:
        node = singles.pop()
        weights = [H0.edges[e]["weight"] for e in H0.edges(node)]
        if len(weights) < 4 and sum(weights) < 5:
            updated = True
            movieno += 1
            M.add_node(movieno, cast="[]")
            if len(weights) < 3 or np.mean(weights) == 1:
                cluster = [n2 for (n1, n2) in H0.edges(node)]
            else:
                tempnodes = sorted([(H0.edges[n0]["weight"],n0) for n0 in H0.edges(node)])
                cluster = [tempnodes[0][1], tempnodes[-1][1]]
            cluster.append(node)
            singles = singles - set(cluster)
            clusteredges = list(combinations((cluster),2))
            while len(cluster) > 0:
                node1 = cluster.pop()
                for node2 in cluster:
                    if weakenedge(H0, node1, node2):
                        if node2 in linkers:
                            linkers[node2].add(movieno)
                        else:
                            linkers[node2] = set([movieno])
                    else:
                        M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H.node[node2]["name"] + ", ]"
                M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H0.node[node1]["name"] + "]"
                if H0.degree(node1) == 0:
                    if node1 in linkers:
                        for linknode in linkers[node1]:
                            M.add_edge(linknode, movieno)
                    H0.remove_node(node1)
                    if node1 in singles:
                        singles.remove(node1)
                else:
                    if node1 in linkers:
                        linkers[node1].add(movieno)
                    else:
                        linkers[node1] = set([movieno])
                
    singles = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
numnodesafter3 = H0.number_of_nodes()
print("After the third clearance, {:,} nodes out of the previous {:,} remain...".format(numnodesafter3,numnodesafter2))
print(nx.info(H0))
print(nx.info(M))
print()

nx.write_graphml(H0, "remaining3.graphml")
nx.write_graphml(M, "converted3.graphml")


# remove the two-node clusters
singles = set([k for (k,v) in nx.clustering(H0).items() if v == 0.0])
while len(singles) > 0:
    node1 = singles.pop()
    node2 = list(H0.edges(node1))[0][1]
    movieno += 1
    M.add_node(movieno, cast="[" + H0.node[node1]["name"] + ", " + H0.node[node2]["name"] + "]")
    if weakenedge(H0, node1, node2):
        if node1 in linkers:
            linkers[node1].add(movieno)
        else:
            linkers[node1] = set([movieno])
        if node2 in linkers:
            linkers[node2].add(movieno)
        else:
            linkers[node2] = set([movieno])
    if H0.degree(node1) == 0:
        if node1 in linkers:
            for linknode in linkers[node1]:
                M.add_edge(linknode, movieno)
        H0.remove_node(node1)
    else:
        singles.add(node1)
    if H0.degree(node2) == 0:
        if node2 in linkers:
            for linknode in linkers[node2]:
                M.add_edge(linknode, movieno)
        H0.remove_node(node2)
        if node2 in singles:
            singles.remove(node2)
numnodesafter4 = H0.number_of_nodes()
print("After the fourth clearance, {:,} nodes out of the previous {:,} remain...".format(numnodesafter4,numnodesafter3))
print(nx.info(H0))
print(nx.info(M))
print()

nx.write_graphml(H0, "remaining4.graphml")
nx.write_graphml(M, "converted4.graphml")

# FROM THIS POINT ON, THINGS ARE NOT CERTAIN. WE ARE PRUNNING THE MOST LIKELY 
# CLIQUES ACCORDING TO A SUBJECTIVE CRITERION


# continue by pruning "most-likely" cliques according to a subjective criterion:
# if a node has a clustering coef. 1, with less than 4 edges 
# and total weighted degree less than 5)

cliquers = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
while len(cliquers):
    # find simplest clusters through fully clustered nodes
    while len(cliquers) > 0:
        node = cliquers.pop()
        
        cluster = [n2 for (n1, n2) in H0.edges(node)]
        cluster.append(node)
        cliquers = cliquers - set(cluster)
        
        clusteredges = list(combinations((cluster),2))
        movieno += 1
        M.add_node(movieno, cast="[]")
        while len(cluster) > 0:
            node1 = cluster.pop()
            for node2 in cluster:
                if weakenedge(H0, node1, node2):
                    if node2 in linkers:
                        linkers[node2].add(movieno)
                    else:
                        linkers[node2] = set([movieno])
                else:
                    M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H.node[node2]["name"] + ", ]"
            M.node[movieno]["cast"] = M.node[movieno]["cast"][:-1] + H0.node[node1]["name"] + "]"
            if H0.degree(node1) == 0:
                if node1 in linkers:
                    for linknode in linkers[node1]:
                        M.add_edge(linknode, movieno)
                H0.remove_node(node1)
                if node1 in cliquers:
                    cliquers.remove(node1)
            else:
                if node1 in linkers:
                    linkers[node1].add(movieno)
                else:
                    linkers[node1] = set([movieno])

#        dummy = transfercluster(H0,M,node)
    cliquers = set([k for (k,v) in nx.clustering(H0).items() if v == 1.0])
numnodesafter5 = H0.number_of_nodes()
print("After the fifthe clearance, {:,} nodes out of the previous {:,} remain...".format(numnodesafter5,numnodesafter4))
print(nx.info(H0))
print(nx.info(M))

nx.write_graphml(H0, "remaining5.graphml")
nx.write_graphml(M, "converted5.graphml")










