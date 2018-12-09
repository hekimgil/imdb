# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 10:20:35 2018

@author: panliu
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
from sklearn.metrics import f1_score
from sklearn.svm import SVC
#%% Reading data
#workpath = input('workpath = ')
workpath = 'D:\OneDrive - HEC Montréal\Course\ComplexNetworkAnalysis\DataIMDB'
with open(os.path.join(workpath, 'df_final.csv')) as csvfile:
    data = pd.read_csv(csvfile)
data.dropna(inplace=True)
#data.fillna(0, inplace=True)
print(data.columns[0:10])
# 440 rows, only 17 become neighbor
#%% Select Data
y = data['become_neighbor'].copy()   #start with stock change within (5~61) days
X = data.drop(['become_neighbor', 'Unnamed: 0'],axis=1).copy()   #
#%% Normalization
print('LogisticRegression regression: effect of alpha regularization parameter\n')
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 0)
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
#%%
from sklearn.dummy import DummyClassifier
dummy_majority = DummyClassifier(strategy = 'most_frequent').fit(X_train_scaled, y_train)
dummy_majority.score(X_test_scaled, y_test)
#%% Logistic Regression
logistic_Cs = {}
for c in [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 10, 20, 30, 40]:
    logistic = LogisticRegression(C = c).fit(X_train_scaled, y_train)
    r2_train = logistic.score(X_train_scaled, y_train)
    r2_test = logistic.score(X_test_scaled, y_test)
    logistic_Cs['{}'.format(c)] = (r2_train, r2_test)
    log_predicted = logistic.predict(X_test_scaled)
    confusion = confusion_matrix(y_test, log_predicted)
    f_score = f1_score(y_test, log_predicted)
    print('c={}'.format(c))
    print(confusion, '\n', f_score)
    print('accuracy training: {:.4f}, accuracy test: {:.4f}\n'
         .format(r2_train, r2_test))
#%% Plot train-test curve
df = pd.DataFrame(logistic_Cs).T
plt.plot(list(map(float,df.index)), df.iloc[:,0], 'r--', label='train')
plt.plot(list(map(float,df.index)), df.iloc[:,1], 'g', label='test')
#plt.axis([0, 0.2, -0.45, 1])  
plt.xlabel('alpha')
plt.ylabel('r-square')
plt.legend()
plt.show()
#%% Confusion matrix
logistic = LogisticRegression(C = 0.01).fit(X_train_scaled, y_train)
y_predicted = logistic.predict(X_test_scaled)
confusion = confusion_matrix(y_test, y_predicted)
confusion
#%%
logistic.coef_
#%% SVM
from sklearn.svm import SVC
for c in [0.005, 0.01, 0.03, 0.05, 0.1, 0.2, 0.25, 0.3, 0.5, 1, 10, 20, 30, 40]:
    svm = SVC(kernel='rbf', C=c).fit(X_train_scaled, y_train)
    svm_predicted = svm.predict(X_test_scaled)
    ac_train = svm.score(X_train_scaled, y_train)
    ac_test = svm.score(X_test_scaled, y_test)
    confusion = confusion_matrix(y_test, svm_predicted)
    f_score = f1_score(y_test, svm_predicted)
    print('c={}'.format(c))
    print(confusion, '\n', f_score)
    print('accuracy training: {:.4f}, accuracy test: {:.4f}\n'
         .format(ac_train, ac_test))
#%%
svm_prd2 = svm.predict(X_test_scaled)
confusion2= confusion_matrix(y_test, svm_prd2)
confusion2
f_score = f1_score(y_test, svm_prd2)
print(confusion2, f_score)
#%% ANN
#%% Oversampling
workpath = 'D:\OneDrive - HEC Montréal\Course\ComplexNetworkAnalysis\DataIMDB'
with open(os.path.join(workpath, 'df_final.csv')) as csvfile:
    data = pd.read_csv(csvfile)
data.dropna(inplace=True)
#data.fillna(0, inplace=True)
data = data.append([data[data['become_neighbor']==1]]*22, ignore_index=True)
print(data.columns[0:10])
# 814rows, 391  become neighbor
#%% Select Data
y = data['become_neighbor'].copy()   #start with stock change within (5~61) days
X = data.drop(['become_neighbor', 'Unnamed: 0'],axis=1).copy()   #
#%% Normalization
print('LogisticRegression regression: effect of alpha regularization parameter\n')
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state = 0)
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
#%%
from sklearn.neural_network import MLPClassifier
for a in [0.0001, 0.001, 0.005, 0.01, 0.03, 0.05, 0.1, 0.5, 1]:
    mlp = MLPClassifier(hidden_layer_sizes=(10,5), alpha=a).fit(X_train_scaled, y_train)
    mlp_predicted = mlp.predict(X_test_scaled)
    ac_train = mlp.score(X_train_scaled, y_train)
    ac_test = mlp.score(X_test_scaled, y_test)
    confusion = confusion_matrix(y_test, mlp_predicted)
    f_score = f1_score(y_test, mlp_predicted)
    print('a={}'.format(a))
    print(confusion, '\n', f_score)
    print('accuracy training: {:.4f}, accuracy test: {:.4f}\n'
         .format(ac_train, ac_test))


