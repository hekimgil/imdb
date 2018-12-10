import community
import networkx as nx
import matplotlib.pyplot as plt
import sqlite3
import pandas as pd
import numpy as np
import operator

arating = 7.0
nvotes = 300


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

def community_layout(g, partition):
    """
    Compute the layout for a modular graph.


    Arguments:
    ----------
    g -- networkx.Graph or networkx.DiGraph instance
        graph to plot

    partition -- dict mapping int node -> int community
        graph partitions


    Returns:
    --------
    pos -- dict mapping int node -> (float x, float y)
        node positions

    """

    pos_communities = _position_communities(g, partition, scale=3.)

    pos_nodes = _position_nodes(g, partition, scale=1.)

    # combine positions
    pos = dict()
    for node in g.nodes():
        pos[node] = pos_communities[node] + pos_nodes[node]

    return pos

def _position_communities(g, partition, **kwargs):

    # create a weighted graph, in which each node corresponds to a community,
    # and each edge weight to the number of edges between communities
    between_community_edges = _find_between_community_edges(g, partition)

    communities = set(partition.values())
    hypergraph = nx.DiGraph()
    hypergraph.add_nodes_from(communities)
    for (ci, cj), edges in between_community_edges.items():
        hypergraph.add_edge(ci, cj, weight=len(edges))

    # find layout for communities
    pos_communities = nx.spring_layout(hypergraph, **kwargs)

    # set node positions to position of community
    pos = dict()
    for node, community in partition.items():
        pos[node] = pos_communities[community]

    return pos

def _find_between_community_edges(g, partition):

    edges = dict()

    for (ni, nj) in g.edges():
        ci = partition[ni]
        cj = partition[nj]

        if ci != cj:
            try:
                edges[(ci, cj)] += [(ni, nj)]
            except KeyError:
                edges[(ci, cj)] = [(ni, nj)]

    return edges

def _position_nodes(g, partition, **kwargs):
    """
    Positions nodes within communities.
    """

    communities = dict()
    for node, community in partition.items():
        try:
            communities[community] += [node]
        except KeyError:
            communities[community] = [node]

    pos = dict()
    for ci, nodes in communities.items():
        subgraph = g.subgraph(nodes)
        pos_subgraph = nx.spring_layout(subgraph, **kwargs)
        pos.update(pos_subgraph)

    return pos

infodf = pd.DataFrame(columns=["period", "# vertices", "# edges", "density", "max degree", "largest CC"])
ccdist = []
top50df = pd.DataFrame(columns=["degree", "name", "birth", "death"])

# read and add actors/actresses
db.execute("SELECT id FROM categories WHERE category = ? OR category = ?;", ("actor","actress"))
catids = db.fetchall()
assert len(catids) == 2

for year1 in range(2000, 2019, 100):
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
        # print(titleid, actid)
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

    print("Initial graph")
    print(nx.info(G))
    if G.number_of_nodes() <= 20:
        print("Nodes: ", G.nodes())
        for node in G.nodes():
            print(G.node[node])
    if G.number_of_edges() <= 20:
        print("Edges: ", G.edges())
    print("Network density:", nx.density(G))
    maxd = max([d for n, d in G.degree()])
    d50th = sorted([d for n, d in G.degree()])[-50]
    print("Maximum degree:", maxd)
    connecteds = list(nx.connected_components(G))
    maxconnectedsize = max([len(c) for c in connecteds])
    print("Size of largest connected component:", maxconnectedsize)
    infodfrow = pd.Series({"period": str(year1) + "-" + str(year2),
                           "# vertices": G.number_of_nodes(),
                           "# edges": G.number_of_edges(),
                           "density": nx.density(G),
                           "max degree": max([d for n, d in G.degree()]),
                           "largest CC": maxconnectedsize},
                          name="Whole graph")
    infodf = infodf.append(infodfrow)
    temp = set([len(c) for c in connecteds])
    temp2 = [len(c) for c in connecteds]
    connectedsizes = {k: temp2.count(k) for k in temp}
    ccdist.append((str(year1) + "-" + str(year2), connectedsizes))
    print("Number of connected components according to size:", connectedsizes)

    # PICK THE LARGEST CONNECTED COMPONENT
    largestcomponents = [cc for cc in connecteds if len(cc) == maxconnectedsize]
    largestcc = largestcomponents[0]
    H = G.subgraph(largestcc).copy()
    G.clear()
    del (G)
    print(nx.info(H))
    print("Network density:", nx.density(H))
    infodfrow = pd.Series({"period": str(year1) + "-" + str(year2),
                           "# vertices": H.number_of_nodes(),
                           "# edges": H.number_of_edges(),
                           "density": nx.density(H),
                           "max degree": max([d for n, d in H.degree()]),
                           "largest CC": maxconnectedsize},
                          name="Largest component")
    infodf = infodf.append(infodfrow)
    top50dact = sorted([(d, n) for n, d in H.degree() if d >= d50th], reverse=True)
    top50dactfull = [(d, peopleinfo(n)) for d, n in top50dact]
    ind = 0
    for t in top50dactfull:
        ind += 1
        death = np.NAN
        if t[1][3] != "NULL":
            death = int(t[1][3])
        birth = np.NAN
        if t[1][2] != "NULL":
            birth = int(t[1][2])
        top50dfrow = pd.Series({"degree": int(t[0]),
                                "name": t[1][1],
                                "birth": birth,
                                "death": death}, name=ind)
        top50df = top50df.append(top50dfrow)

    # G.clear()
    count_com = {}
    #first compute the best partition
    partition = community.best_partition(H)
    no_partition = max(partition) + 1
    print(partition)

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


    pos = community_layout(H, partition)

    for i in range(no_partition):
        selected_nodes = [n for n in H.nodes() if partition[n] == i]
        print(selected_nodes)
        N = H.subgraph(selected_nodes)
        labels = {}
        for node in N.nodes():
            # set the node name as the key and the label as its value
            labels[node] = N.node[int(node)]["name"]
        plt.figure(i)
        nx.draw(N, pos, with_labels=False, node_size=10)
        nx.draw_networkx_labels(N, pos, labels, font_size=6)
        #plt.show()
        filename = "C:\\Users\\Cansu\\Desktop\\CN_Project\\graphs\\community%d.graphml" % i
        nx.write_graphml(N, filename)
    #selected_edges = [(u, v) for u, v, e in G.edges(data=True) if e['since'] == 'December 2008']
    #print(selected_edges)
    #N = nx.Graph(((u, v, e) for u, v, e in N.edges_iter(data=True) if e['since'] == 'December 2008'))
    #nx.draw(N, with_labels=False, node_size=10)

    #nx.draw(H, pos, node_color=list(partition.values()), node_size=10, )
    #plt.show()
    #nx.draw_networkx_nodes(H, pos, node_size=10, cmap=plt.cm.RdYlBu, node_color=list(partition.values()),with_labels=False)
    #nx.draw_networkx_labels(H, pos, labels, font_size=6)
    #plt.show(H)




