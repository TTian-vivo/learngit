# -*- coding: utf-8 -*

import numpy as np
import faiss
import time
import os
import json
import hdfs
from flask import Flask,request

now = time.strftime("%Y-%m-%d", time.localtime())
app = Flask(__name__)

@app.route("/create_index/", methods=['GET'])
def create_index():
    d_str = request.args.get('dim', '128')
    d = int(d_str)
    const = request.args.get('const')
    MetricType = request.args.get('MetricType')
    path = request.args.get('path')
    url_tmp = request.args.get('url', 'http://10.21.52.65:50070;http://10.20.94.70:50070')
    now = time.strftime("%Y-%m-%d", time.localtime())
    Client = hdfs.InsecureClient(url=url_tmp, user='hdfs')
    fm_adid_embedding_dict = {}
    j = 1
    all_data_str = ""
    for dirpath, dirnames, filenames in Client.walk(path):
        for filepath in filenames:
            print(dirpath + '/' + filepath)
            with Client.read(dirpath + '/' + filepath) as reader:
                all_data_hdfs = reader.read()
                data_str = bytes.decode(all_data_hdfs)
                all_data_str = all_data_str + data_str
    data_split = all_data_str.split("\n")
    for line in data_split:
        line_split = line.split(",")
        adid = line_split[0]
        list_one = []
        if (len(line_split) > 10):
            i = 1
            while i <= len(line_split) - 1:
                list_one.append(float(line_split[i]))
                i = i + 1
            fm_adid_embedding_dict[adid] = list_one

    list_adid = fm_adid_embedding_dict.keys()
    data = []
    for value in fm_adid_embedding_dict.values():
        data.append(value)
        if MetricType == "COS":
            xb = np.array(data).astype('float32')
            global index
            index = faiss.index_factory(d, const, faiss.METRIC_INNER_PRODUCT)
    index.is_trained
    index.train(xb)
    index.add(xb)
    return "success"

@app.route("/faiss_service/", methods=['GET'])
def faiss_service():
    try:
        print("start get data")
        nprobe = int(request.args.get("nprobe"))
        top_K = int(request.args.get("top_n"))
        data_string = request.args.get("data")
        data_string_split = data_string.split(",")
        print("get data over")

        query_data = []
        for i in range(len(data_string_split)):
            query_data.append(data_string_split[i])

        xq = np.array([query_data]).astype('float32')

        start_search_time = time.time()
        index.nprobe = nprobe
        D, I = index.search(xq, top_K)
        end_search_time = time.time()
        print("search time ："+str(end_search_time - start_search_time))

        start_my_method_time = time.time()
        D_I_couple = []
        for i in range(len(I[0])):
            D_I_couple.append((str(D[0][i]), str(I[0][i])))
        end_my_method_time = time.time()
        print("data add time ：" + str(end_my_method_time - start_my_method_time))

        result_data = {}
        result_data["status"] = "success"
        result_data["results"] = D_I_couple
        result_json = json.dumps(result_data)
        return result_json

    except AssertionError as e1:
        return e1
    except ValueError as e2:
        return e2

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, use_reloader=False)