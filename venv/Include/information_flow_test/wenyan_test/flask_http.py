#coding:utf-8
#该脚本的功能是从hdfs获取数据并生成index，然后保存在本地

import os
import time
import numpy as np
import faiss
import hdfs
from hdfs.client import Client
from flask import Flask, jsonify, request, make_response
from time import sleep
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
executor = ThreadPoolExecutor(2)

@app.route("/make_index_p1/")
def make_index_p1():
    d_str = request.args.get('dim', '128')
    const = request.args.get('const')
    MetricType = request.args.get('MetricType')
    path = request.args.get('path')
    url = request.args.get('url', 'http://10.21.52.65:50070;http://10.20.94.70:50070')

    executor.submit(long_task,d_str,const,MetricType,path,url)

    return 'success'

@app.route("/exit_index/")
def exit_index():
    os.system("kill -9 `ps aux|grep -v grep|grep faiss_make_index.py|awk '{print $2}'`")
    os._exit(0)

def long_task(d_str,const,MetricType,path,url):
    now = time.strftime("%Y-%m-%d", time.localtime())#获取现在的日期
    d = int(d_str)
    Client = hdfs.InsecureClient(url=url, user='hdfs')
    fm_adid_embedding_dict = {}  # adid 与 数据的字典
    j = 1

    # 处理hdfs数据
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

    list_adid = fm_adid_embedding_dict.keys()  # 包含adid的list
    # 构建数据，格式为连续二维数组，每个数值必须为float32类型
    data = []
    for value in fm_adid_embedding_dict.values():
        data.append(value)
    if MetricType == "COS":
        xb = np.array(data).astype('float32')
        index = faiss.index_factory(d, const, faiss.METRIC_INNER_PRODUCT)
    elif MetricType == "L2":
        xb = np.array(data).astype('float32')
        index = faiss.index_factory(d, const, faiss.METRIC_L2)
    elif MetricType == "Binary":
        xb = np.array(data).astype('uint8')
        index = faiss.index_binary_factory(d, const)
    else:
        return "input of MetricType is ERROR !!!!"
    index.is_trained
    index.train(xb)
    index.add(xb)
    #TODO 修改路径名
    faiss.write_index(index, "/home/app/ttian/index_of_p1_"+now)
    print("index is ready")


if __name__ == '__main__':
    #todo 修改IP
    app.run(host='0.0.0.0', port='8000', debug=True, use_reloader=False)
