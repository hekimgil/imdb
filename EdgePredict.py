# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 22:31:51 2018

@author: panliu
"""
import numpy as np
import pandas as pd
import sqlite3
import networkx as nx
import pickle
#%% 
def become_neighbor(act_id, candidate_id):
    return candidate_id in G2.neighbors(act_id)

H = G1
k = 0 
def candidates(actor):
    name = actor
    act_id = findperson(name)[0]
    non_neighbors = nx.non_neighbors(H, act_id)
    pairs = [(act_id, n) for n in non_neighbors]
    # Common Neighbors
    non_neighbors = nx.non_neighbors(H, act_id)
    common_neigh= [(n, len(list(nx.common_neighbors(H, act_id, n)))) for n in non_neighbors]
    common_neigh.sort(key=lambda n: n[1], reverse=True)
    df_cn = pd.DataFrame(common_neigh[0:20]).set_index(keys=0)
    df_cn.columns=['common_neigh']
    print('common_neigh done')
    # Jaccard Coefficient
    L = nx.jaccard_coefficient(H, pairs)
    jacc = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L if act_id in (n[0], n[1])]
    jacc.sort(key=lambda n: n[1], reverse=True)
    df_jacc = pd.DataFrame(jacc[0:20]).set_index(keys=0)
    df_jacc.columns=['jacc']
    print('jaccard done')
    # Resource Allocation
    L2 = list(nx.resource_allocation_index(H, pairs))
    resource_allo = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L2 if act_id in (n[0], n[1])]
    resource_allo.sort(key=lambda n: n[1], reverse=True)
    df_ra = pd.DataFrame(resource_allo[0:20]).set_index(keys=0)
    df_ra.columns=['resource_allo']
    print('resource done')
    # Adamic-Adar Index
    L3 = list(nx.adamic_adar_index(H, pairs)) 
    adamic_adar = [(n[1], n[2]) if n[0]==act_id else (n[0], n[2]) for n in L3 if act_id in (n[0], n[1])]
    adamic_adar.sort(key=lambda n: n[1], reverse=True)
    df_aa = pd.DataFrame(adamic_adar[0:20]).set_index(keys=0)
    df_aa.columns=['adamic_adar']
    print('Adamic done')
    # Merge
    df = pd.concat([df_cn, df_jacc, df_ra, df_aa], axis=1, join='outer', sort=False)
    # Add Degree
    df['degree'] = [G1.degree[n] for n in df.index]
    # If Become Neighbor
    df['become_neighbor'] = [1 if become_neighbor(act_id, n) else 0 for n in df.index]
    global k
    k +=1
    print('actor {} done'.format(k))
    return df
#%% Main
with open(r'D:\OneDrive - HEC Montréal\Course\ComplexNetworkAnalysis\DataIMDB\Top150MaleActionStars.csv') as f:
    top150 = pd.read_csv(f)
#%%
top150['Birth Date'] = pd.to_datetime(top150['Birth Date'])
pic = top150['Birth Date'].groupby(top150['Birth Date'].dt.year).count()
pic.plot(kind='bar', figsize=(18,14))

#%%
actors = list(top150['Name'])
i = 0
list_df = [candidates(a) for a in actors if findperson(a)[0] in G1 and findperson(a)[0] in G2]
df_final = pd.concat(list_df, ignore_index='False')
df_final.rename(columns = {'Unnamed: 0':'act_id'}, inplace=True)
df_final.to_csv('df_final_2.csv')




