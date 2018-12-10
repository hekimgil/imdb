# -*- coding: utf-8 -*-
"""
Python code to read .tsv İMDB data and store it in a sqlite database

This model assumes actors/actresses as nodes and common movies as weighted
edges. Different 20-year periods are used to set up separate networks and basic
data such as number of nodes, number of edges, and distribution of sizes
of connected components are examined to get a general idea on various 20-year
windows.

VERSION A: PROCESSES THE WHOLE MOVIE DATABASE INSTEAD OF 20-YEAR WINDOWS.

@author: Hakan Hekimgil
"""

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
def connections(act1, act2):
    return "\n".join([peopleinfo(x)[1] for x in nx.dijkstra_path(G, act1, act2, weight=None)])
def connectionslist(act1, act2):
    return [peopleinfo(x)[1] for x in nx.dijkstra_path(G, act1, act2, weight=None)]
def nameconnect(act1, act2):
    return [peopleinfo(x)[1] for x in nx.dijkstra_path(G, findperson(act1)[0], findperson(act2)[0], weight=None)]
# sample connections
samples=[(    102,    559), #  0: Kevin Bacon - Leonard Nimoy
         (    102,1486647), #  1: Kevin Bacon - Álvaro Morte
         (    102, 352032), #  2: Kevin Bacon - Kamal Haasan
         (    102,     12), #  3: Kevin Bacon - Bette Davis
         (    102, 559144), #  4: Kevin Bacon - Marlee Matlin
         (    102,    300), #  5: Kevin Bacon - Juliette Binoche
         (    102, 433495), #  6: Kevin Bacon - Ernst-Hugo Järegård
         (    102, 375138), #  7: Kevin Bacon - Sofia Helin
         (    102, 557908), #  8: Kevin Bacon - Oliver Masucci
         (    102,   1536), #  9: Kevin Bacon - Toshirô Mifune
         (    102, 947447), # 10: Kevin Bacon - Donnie Yen
         (    102, 851876), # 11: Kevin Bacon - Ferdi Tayfur
         (    102, 960385), # 12: Kevin Bacon - Ayhan Isik
         (    102,     52), # 13: Kevin Bacon - Marcello Mastroianni
         (    102,    122), # 14: Kevin Bacon - Charles Chaplin
         (    102, 948000), # 15: Kevin Bacon - Cem Yilmaz
         (    102, 839017), # 16: Kevin Bacon - Kemal Sunal
         (    102,     80), # 17: Kevin Bacon - Orson Welles
         (    102,    488), # 18: Kevin Bacon - Brandon Lee
         (    102,   1472), # 19: Kevin Bacon - Jet Li
         (    102,    241), # 20: Kevin Bacon - Jean-Claude Van Damme
         (    102,    901), # 21: Kevin Bacon - Jean-Paul Belmondo
         (    102,     86), # 22: Kevin Bacon - Louis de Funès
         (    102,      7), # 23: Kevin Bacon - Humphrey Bogart
         (    102,      1), # 24: Kevin Bacon - Fred Astaire
         (    102,     78), # 25: Kevin Bacon - John Wayne
         (    102,1785339), # 26: Kevin Bacon - Rami Malek
         (    102, 461498), # 27: Kevin Bacon - Beyoncé
         (    102,     62), # 28: Kevin Bacon - Elvis Presley
         (    102,   1654), # 29: Kevin Bacon - Ronald Reagan
         ( 874028,   1654), # 30: Catherine Trudeau - Ronald Reagan
         ( 698949,     62), # 31: Émile Proulx-Cloutier - Elvis Presley
         (1817061, 461498), # 32: Édith Cochrane - Beyoncé
         (2954178,   4896), # 33: Pier-Luc Funk - Eminem
         ( 324077,1982597), # 34: Patrice Godin - Rihanna
         ( 189887,     86), # 35: Marie-Josée Croze - Louis de Funès
         ( 479745,   1776), # 36: Marc Labrèche - Sting
         (  99886,1785339), # 37: Hélène Bourgeois Leclerc - Rami Malek
         ( 223518,     28), # 38: Caroline Dhavernas - Rita Hayworth
         ( 681401,     78), # 39: Luc Picard - John Wayne
         (1234157,   4874), # 40: Stéphane Rousseau - Vin Diesel
         ( 495501,   1541), # 41: Pierre Lebeau - Kylie Minogue
         ( 495799,1620783), # 42: Laurence Leboeuf - Meghan Markle
         ( 246386,   5286), # 43: Mélissa Désormeaux-Poulin - Haley Joel Osment
         ( 223518,   1257), # 44: Caroline Dhavernas - Ava Gardner
         ( 888468,    329), # 45: Karine Vanasse - Jackie Chan
         (2903342,    437), # 46: Evelyne Brochu - Woody Harrelson
         ( 495920,     45), # 47: Julie LeBreton - Bruce Lee
         ( 551885, 316284), # 48: Alexis Martin - Giancarlo Giannini
         ( 399088,      3), # 49: Patrick Huard - Brigitte Bardot
         (1481355,      7), # 50: Louis-José Houde - Humphrey Bogart
         ( 499218,      1), # 50: Claude Legault - Fred Astaire
         (      1,      2)]

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "weights", "density", 
                               "max deg.", "max deg. (w)", "largest CC"])
ccdist = []
top50df = pd.DataFrame(columns=["degree", "name", "birth", "death"])
top50dfw = pd.DataFrame(columns=["degree", "name", "birth", "death"])

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(1930,2010,100):
    year2 = year1 + 99
    G.clear()
    
    # read people and titles for a 20-year window
    db.execute(
            "SELECT DISTINCT t.tconst, nconst " + 
            "FROM (SELECT nconst, tconst FROM principals WHERE catid = ? OR catid = ?) AS p " + 
            "INNER JOIN (SELECT tconst FROM titles WHERE startYear >= ? AND startYear <= ?) AS t " + 
            "ON t.tconst = p.tconst " + 
            "ORDER BY t.tconst;", (catids[0][0], catids[1][0], year1, year2))
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
    top50dact = sorted([(d,n) for n,d in G.degree() if d >= d50th], reverse=True)
    top50dactfull = [(d,peopleinfo(n)) for d,n in top50dact]
    ind=0
    for t in top50dactfull:
        ind += 1
        death = np.NAN
        if t[1][3] != "NULL":
            death = int(t[1][3])
        top50dfrow = pd.Series({"degree":int(t[0]), 
                                "name":t[1][1], 
                                "birth":int(t[1][2]),
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
print("\n\nActors/actresses with maximum degrees:")
print("--------------------------------------")
print(top50df)
#print(ccdist)
