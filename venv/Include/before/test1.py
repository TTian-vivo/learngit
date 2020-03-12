from flask import Flask, jsonify, request,make_response
import numpy as np
import faiss
import time
import os
import json
import multiprocessing

now=time.strftime("%Y-%m-%d", time.localtime())
port_server=8080
path_index="/data/ai-recall-faiss/approot/data/index_2020-02-19"
path_adid="/data/ai-recall-faiss/approot/data/arr_adid_2020-02-19.npy"

#get the data from disk
try:
    #global index
    index = faiss.read_index(path_index)
    #global list_adid
    list_adid = list(np.load(path_adid))
except (FileNotFoundError, RuntimeError):
    print("file of index is not exit !")

def add_index(xq_add):
    index.add(xq_add)
    num = index.ntotal
    print("add data success")
    print(num)
    return index

app = Flask(__name__)

@app.route("/faiss_service/", methods=['GET', 'POST'])
def faiss_service():
    try:
        if request.method == 'POST':
            http_input = request.get_data()
            http_dict = json.loads(http_input)
            MetricType = http_dict['MetricType']
            nprobe_input = http_dict['nprobe']
            top_n_input = http_dict['top_n']
            data_input = http_dict['data']
        elif request.method == 'GET':
            MetricType = request.args.get('MetricType')
            nprobe_input = request.args.get('nprobe', "32")
            top_n_input = request.args.get("top_n")
            data_input = request.args.get("data")

        #split multi data
        data_list =data_input.split(";")
        k = int(top_n_input)
        nprobe = int(nprobe_input)
        list_test_adid = []
        list_data = []
        for i in data_list:
            test_data_split = i.split(",")
            list_test_adid.append(test_data_split[0])
            list_data.append(test_data_split[1:])
        if MetricType == "L2" or MetricType == "COS":
            #change into np.array
            xq = np.array(list_data).astype('float32')
        elif MetricType == "Binary":
            xq = np.array(list_data).astype('uint8')
        else:
            return "input of MetricType is ERROR !!!!"

        #nprobe,decide the accurcy
        index.nprobe = nprobe
        #search，D:distance，I:index
        D, I = index.search(xq, k)

        #define return data
        result_data = {}
        result_data["status"] = "success"
        result_list = {}
        length = D.shape[0]
        for i in range(length):
            result_list_tmp = {}
            n = 0
            for j in I[i]:
                result_list_tmp[list_adid[j]] = str(D[i][n])
                n = n + 1
            result_list[list_test_adid[i]] = result_list_tmp

        key = "results"
        result_data[key] = result_list
        vector_num = "vector_num"
        result_data[vector_num] = index.ntotal
        result_json = json.dumps(result_data)
        return result_json
    except AssertionError:
        return "the args input error!"
    except ValueError:
        return "value error!"


@app.route("/add_data/", methods=['GET', 'POST'])
def add_data():
    try:
        if request.method == 'GET':
            MetricType = request.args.get('MetricType')
            data_input = request.args.get('data')
        elif request.method == 'POST':
            http_input = request.get_data()
            http_dict = json.loads(http_input)
            MetricType = http_dict['MetricType']
            data_input = http_dict['data']

        #split data
        data_split = data_input.split(";")
        add_data_list = []
        for i in data_split:
            add_data = i.split(",")
            list_adid.append(add_data[0])
            add_data_list.append(add_data[1:])
        if MetricType == "COS":
            #change data into np.array
            xq_add = np.array(add_data_list).astype('float32')
        else:
            return "input of MetricType is ERROR !!!!"

        index_ = multiprocessing.Value('np.array',index)
        thread = multiprocessing.Process(target=add_index, args=(xq_add,))
        thread.start()

        #define return data
        index_num=index.ntotal
        result_data={}
        result_data["status"] = "success"
        result_data["vector_num"] = index_num
        result_json = json.dumps(result_data)
        return result_json
    except Exception:
        return "ADD DATA ERROR!!!!!"

@app.route("/faiss_exit/")
def faiss_exit():
    os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
    os._exit(0)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port_server, debug=True, use_reloader=False)
