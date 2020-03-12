# 功能：读取index和adid列表，然后对外发送http服务，接收输入，返回最相似的结果的adid
# 输入格式：k值和str。
# k：返回的相似个数；
# str需要查找的数据，格式：支持同时查找多个数据，多个数据之间用“；”间隔，数据内部用“，”间隔

from flask import Flask, jsonify, request
import numpy as np
import faiss
import time
import os
import json
import hdfs
import requests
import gc
import sys

def create_index(path):
    # 构建连接
    url_tmp = 'http://10.193.7.141:50070;http://10.193.7.21:50070'
    print("################################################")
    print("start_update_index")
    global index
    Client = hdfs.InsecureClient(url=url_tmp, user='hdfs')

    # 读取数据
    with Client.read(path) as reader:
        data_hdfs = reader.read()

    # 数据处理
    data_str = bytes.decode(data_hdfs).replace("\"[", "").replace("]\"", "")
    data_split = data_str.split("\n")
    del data_str
    del data_hdfs
    gc.collect()

    data_list = []
    index_list = []
    for line in data_split:
        if line != "":
            line_split = line.split(",")
            data_str_tmp = line_split[3:]
            index_temp = line_split[2]
            data_list.append(data_str_tmp)
            index_list.append(index_temp)

    # 构建索引
    xb = np.array(data_list).astype('float32')
    ids = np.array(index_list).astype('int')
    d = 128
    start_index_time = time.time()
    index_tmp = faiss.IndexFlatL2(d)
    index = faiss.IndexIDMap(index_tmp)
    end_index_time = time.time()

    start_add_time = time.time()
    index.add_with_ids(xb, ids)
    end_add_time = time.time()
    print(index.ntotal)
    print("send success")
    print("the index time is :::", end_index_time - start_index_time)
    print("the index add time is :::", end_add_time - start_add_time)

try:
    path_json = requests.get("http://v-deploy-faiss-dis-test:5000/register/")
    #path_json = requests.get("http://172.17.0.2:5000/register/")
except requests.ConnectionError as e:
    print(e)
path = json.loads(path_json.text)['path']
print(path)
print("*****************************************")

create_index(path)

now = time.strftime("%Y-%m-%d", time.localtime())  # 获取现在的日期
app = Flask(__name__)

@app.route("/faiss_service/", methods=['GET', 'POST'])
def faiss_service():
    try:
        if request.method == 'POST':
            http_input = request.get_data()
            http_dict = json.loads(http_input)
            # nprobe_input = http_dict['nprobe']
            top_n_input = http_dict['top_n']
            data_input = http_dict['data']
        elif request.method == 'GET':
            # nprobe_input = request.args.get('nprobe', "1")
            top_n_input = request.args.get("top_n")
            data_input = request.args.get("data")

        data_string = data_input.split(",")
        data_list = []
        for i in range(len(data_string)):
            data_list.append(float(data_string[i]))

        k = int(top_n_input)
        xq = np.array([data_list]).astype('float32')

        result_list = []
        start_time_search = time.time()
        D, I = index.search(xq, k)
        end_time_search = time.time()
        app.logger.info("从节点数据搜索时间："+str(end_time_search-start_time_search))

        start_add_data_time = time.time()
        for j in range(len(I)):
            D_str = [str(val) for val in D[j]]
            I_str = [str(val) for val in I[j]]
            D_I = list(zip(D_str, I_str))
            result_list.append(D_I)
        # D_I_couple = []
        # for i in range(len(I[0])):
        #     D_I_couple.append((str(D[0][i]), str(I[0][i])))
        end_add_data_time = time.time()
        app.logger.info("从节点数据添加时间："+str(end_add_data_time-start_add_data_time))

        result_data = {}
        result_data["status"] = "success"
        result_data["results"] = result_list
        result_json = json.dumps(result_data)
        return result_json
    except (AssertionError):
        return "input error!!! 传入的维数不对"
    except ValueError:
        return "input error!!! 传参方式错误"

@app.route("/check_slave/",methods=['GET'])
def check_slave():
    result_data = {}
    result_data["status"] = "success"
    result_data["message"] = "I am ready"
    json_message = json.dumps(result_data)
    return json_message

@app.route("/faiss_exit/")
def faiss_exit():
    os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
    os._exit(0)


if __name__ == '__main__':

    # 开启服务
    app.run(host='0.0.0.0', port=5051, debug=True, use_reloader=False)



