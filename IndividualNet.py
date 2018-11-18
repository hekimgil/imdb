# -*- coding: utf-8 -*-
"""
Funtion to draw the subgraph of a given artist together with all his/her neighbors. 
Might be interesting for studying a particular person.
Using the function and graph built from model1a.py, so need to run that first before using this funtion.

Created on Fri Nov 16 23:08:37 2018
@author: panliu
"""


#%%
#print(findperson('Bruce Lee'))
#%%
def individualNet(Name=None):
    if Name == None:
        name = input('Input the name:')
    else: 
        name = Name
    actid = findperson(name)[0]
    neighbors = [n for n in G[actid]]
    neighbors.append(actid)
    G_sub = G.subgraph(neighbors)
    #Draw network. Parameters can be changed as needed.
    plt.figure(figsize=(15,15))
    node_size = [10*G_sub.degree(v) for v in G_sub]
    pos = nx.spring_layout(G_sub)
    edge_width = [2*G_sub[u][v]['weight'] for u,v in G_sub.edges()]
    nx.draw_networkx(G_sub, pos=pos, alpha=0.7, node_size=node_size, width=edge_width, node_color='b')
    #origin_edges = [x for x in G.edges(data=True) if x[1]==actid or x[0]==actid]
    #nx.draw_networkx_edges(G, pos=pos, edgelist=origin_edges, edge_color='g', alpha=0.4, width=2)
    return
individualNet()