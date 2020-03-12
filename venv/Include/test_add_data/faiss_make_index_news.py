#coding:utf-8
#该脚本的功能是从hdfs获取数据并生成index，然后保存在本地

import os
import time
import numpy as np
import faiss
import hdfs
from hdfs.client import Client
from flask import Flask, jsonify, request, make_response
from concurrent.futures import ThreadPoolExecutor
import json
import redis

app = Flask(__name__)
executor = ThreadPoolExecutor(2)
logger = logging.getLogger('flask.app')

@app.route("/check.do")
def get():
    resp = make_response("ok")
    resp.status = "200"
    return resp

#服务1，拉取hdfs数据构建索引
@app.route("/make_index_p1/")
def make_index_p1():
    d_str = request.args.get('dim', '128')
    const = request.args.get('const')
    MetricType = request.args.get('MetricType')
    path = request.args.get('path')
    url_tmp = request.args.get('url', 'http://10.21.52.65:50070;http://10.20.94.70:50070')

    executor.submit(make_index_task_p1,d_str,const,MetricType,path,url_tmp)
    return "success"

#向索引中添加数据
@app.route("/add_data_p1/")
def add_data_p1():
    try:
        if request.method == 'POST':
            http_input = request.get_data()
            http_dict = json.loads(http_input)
            data_input = http_dict['data']
        elif request.method == 'GET':
            data_input = request.args.get("data")
        print("message is ready to publish in main thread")
        redis_publish_add_data_p1(data_input)
        print("message is publish in main thread")
        return "receive data success!"
    except Exception:
        return "receive data error!"

@app.route("/exit_index/")
def exit_index():
    #todo 修改被杀进程的程序名称
    os.system("kill -9 `ps aux|grep -v grep|grep faiss_make_index_news.py|awk '{print $2}'`")
    os._exit(0)

def make_index_task_p1(d_str,const,MetricType,path,url_tmp):
    now = time.strftime("%Y-%m-%d", time.localtime())  # 获取现在的日期
    d = int(d_str)
    Client = hdfs.InsecureClient(url=url_tmp, user='hdfs')
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
    # 阅图专用
    # data_split = all_data_str.replace("\t", ",").split("\n")
    # 通用
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
    # TODO 修改路径名
    faiss.write_index(index, "/data/ai-news-faiss-index/approot/data/index_of_p1_" + now)
    arr_adid = np.array(list(list_adid))
    np.save("/data/ai-news-faiss-index/approot/data/arr_adid_of_p1_" + now + ".npy", arr_adid)  # 保存adid

    # 上传index文件到sftp，并发送redis消息
    os.system("bash /data/ai-news-faiss-index/approot/bin/sftp_p1.sh")
    # todo 测试环境下需要修改anaconda的版本
    os.system('/home/app/anaconda/bin/python /data/ai-news-faiss-index/approot/bin/redis_publish_of_p1.py')  # Redis发布
    # bash /data/ai-rec-faiss-index/approot/bin/restart_master.sh > /data/ai-rec-faiss-index/logs/biz/restart_master_log 2>&1 &
    os.system(
        "bash /data/ai-news-faiss-index/approot/bin/restart_master.sh > /data/ai-news-faiss-index/logs/biz/restart_master_log 2>&1 & ")
    logger.info("p1 index is ready")

def redis_publish_add_data_p1(data_input):
    # 功能：发送index准备好了的信号
    rc = redis.StrictRedis(host='10.193.22.154', port='11110', db=0)
    value = "p1:" + data_input
    rc.publish("faiss_channel_python_news_add_data", value)
    print("message is publish")

if __name__ == '__main__':
    #启动服务
    app.run(host='0.0.0.0', port='8080', debug=True, use_reloader=False)