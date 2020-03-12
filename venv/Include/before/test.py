# import requests
#
# add_data = "V0100000020200210A00PV3,-1.5053489,1.0504726,2.5282655,-1.614492,3.947578,2.3389769,3.195106,2.4118552,1.804597,2.1658738,4.697333,0.12376599,6.810522,-4.136545,6.796263,6.288955,-5.1500993,3.7436397,-4.323357,2.7772524,-3.9519136,6.4990005,-5.2154384,4.2450576,-5.1534667,0.1208011,0.6006905,-3.981073,0.87267816,-1.9640057,1.7520753,-3.2275114,1.2642676,1.6830379,-4.429536,-0.67859167,-1.0354838,-5.646251,-6.067704,-2.9030192,0.5390334,-0.74336517,-0.70042217,2.3178036,2.4483004,3.2590654,-1.5635815,4.043894,0.809255,4.4304967,-3.9670446,-2.206839,5.1362195,-5.087981,0.4319175,1.7331313,3.431356,0.53321165,-3.6411397,0.99572635,-1.8348647,2.07061,0.94120514,4.2568264,-1.2349775,3.9727437,2.2691946,0.24934186,4.8526006,-4.8474145,-3.2986872,3.0481415,-6.0733833,4.4278255,-6.8280125,1.0002415,6.575487,-6.0698175,5.5424867,3.0201483,-0.6565585,-1.6205575,2.6284165,2.7350662,-4.1583366,-1.6550429,0.25883612,-1.4890357,1.6737283,-1.019838,-5.938089,0.16346574,-2.639663,-1.4369284,-6.5001225,-4.884468,-4.334979,2.5805337,-1.4456398,6.412505,0.43476152,4.4909487,5.4394975,0.80442,-0.28604516,-0.086785525,-3.7469323,-1.1451616,2.803277,-4.845011,-2.0049691,4.2907987,0.93983,-0.28285658,4.4148955,-5.423498,-2.126418,-0.67472905,-1.1549724,1.4799865,0.3498147,6.388602,4.378893,-3.285292,3.5597472,6.5552735,-0.37746844,-1.7988105"
#
# a = add_data.split(",")
#
# url ="http://10.101.15.252:8080/add_data_p1/"
# r = requests.get(request, params=re_data)

#from flask import Flask, jsonify
# from multiprocessing import Value,Process
# import time
#
#
# def add_person(person):
#     person.value=20
#
#
# if __name__ == '__main__':
#     person = Value('i',10)
#     print(person.value)
#     thread = Process(target=add_person, args=(person,))
#     thread.start()
#     thread.join()
#     print(person.value)
import multiprocessing
#import faiss
import numpy as np

def get_index():
    d = 10
    nb = 100
    xb = np.random.random((nb,d)).astype("float32")
    nlist = 2
    index1 = faiss.IndexFlatL2(d)
    global index
    index = faiss.IndexIVFFlat(index1,d,nlist)
    index.train(xb)
    index.add(xb)
    return True

def fun(ns,x,y):
    x.append(1)
    y.append('x')
    ns.x = x
    ns.y = y


if __name__ == '__main__':
    d = 10
    nb = 100
    xb = np.random.random((nb, d)).astype("float32")
    nlist = 2
    index1 = faiss.IndexFlatL2(d)
    index = faiss.IndexIVFFlat(index1, d, nlist)
    index.train(xb)
    index.add(xb)
    num = index.ntotal
    manager = multiprocessing.Manager()
    ns = manager.Namespace()
    print(ns)
    ns.x = []
    ns.y = []
    print("before",ns)
    p = multiprocessing.Process(target=fun,args=(ns,ns.x,ns.y,))
    p.start()
    p.join()
    print("after",ns)