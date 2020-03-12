import hdfs
import requests
import json
import time
import multiprocessing

def get_data():
    ##取数据的过程
    path='/region05/29962/app/develop/recall_alg'
    url='http://10.193.7.159:50070;http://10.193.7.35:50070'
    Client = hdfs.InsecureClient(url=url, user='hdfs')

    #从hdfs读取数据
    all_data_str = ""
    for dirpath, dirnames, filenames in Client.walk(path):
        for filepath in filenames:
            print(dirpath + '/' + filepath)
            with Client.read(dirpath + '/' + filepath) as reader:
                all_data_hdfs = reader.read()
                data_str = bytes.decode(all_data_hdfs)
                all_data_str = all_data_str + data_str

    #数据读取后，做数据处理，处理为adid和vector的格式
    adids_vectors_ = all_data_str.split("\n")
    global adids_vectors
    adids_vectors= [x for x in adids_vectors_ if x != '']

if __name__ == '__main__':
    get_data()
    start=time.time()

    body={}
    for adid_vector_tmp in adids_vectors:
        adid_vector = adid_vector_tmp.split("\t")
        adid = adid_vector[0]

