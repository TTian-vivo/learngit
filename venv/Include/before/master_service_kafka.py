"""
/*
    本服务功能：
    1.接收业务端的请求并解析；
    2.解析后，将被查询向量与质心向量做相似计算，并返回TopK;
    3.发送特征向量到相应节点；
    4.接收slave返回的TopK；
    5.查询，映射回adid并返回;

    新增功能：
    1.从节点注册ip到主节点；
    2.在主节点构建：质心和ip的映射、路径和ip的映射；
    3.定时监控，一段时间主节点发送一次信息给从节点，若有返回，则健康，若无返回，就将对应的ip置为0；并开启重启
    4.增加重启机制，若发现有节点挂掉，将ip为0 的对应的路径取出，重新赋给新开启的节点
*/
"""

from flask import Flask, request
from hdfs import InsecureClient, Client
import numpy as np
import time
import os
import json
import requests
import sys
import multiprocessing
from kafka import KafkaProducer
from kafka.errors import KafkaError

now = time.strftime("%Y-%m-%d", time.localtime())  # 获取现在的日期
app = Flask(__name__)


# lock = multiprocessing.Lock()

def get_adid_index_data(client, path_adid_index):
    dict_adid_index = {}
    try:
        with client.read(path_adid_index) as reader:
            data_hdfs = reader.read()
            data_str_all = bytes.decode(data_hdfs)
            line_split_all = data_str_all.split("\n")
            for i in line_split_all:
                if i == '':
                    line_split_all.remove(i)
                else:
                    pass
            for line in line_split_all:
                line_split = line.replace("(", "").replace(")", "").split(",")
                adid = line_split[0]
                adid_index = line_split[1]
                dict_adid_index[adid_index] = adid
        print("the adid_index data is ready!")
    except IOError as e:
        print(e)
    return dict_adid_index


def get_centreids_data(client, path_centreids_vector):
    dict_centreids_vector = {}
    try:
        with client.read(path_centreids_vector) as reader:
            data_hdfs = reader.read()
            data_str_all = bytes.decode(data_hdfs)
            line_split_all = data_str_all.split("\n")
            for line in line_split_all[:-1]:
                line_replace = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("\n",
                                                                                                                "")
                line_split = line_replace.split(",")
                len_line = len(line_split)
                centreid_vector = []
                centreid = int(line_split[0])
                for i in range(1, len_line):
                    centreid_vector.append(float(line_split[i]))
                dict_centreids_vector[centreid] = centreid_vector
        print("the centroids data is ready!")
    except IOError as e:
        print(e)
    return dict_centreids_vector


# 定义维度
dim = 128
# 自定义一些后面需要用的映射字典
ip_path_dict = {}
path_ip_dict = {}

path_centreid_dict = {}
ip_centreid_dict = {}
centreid_ip_dict = {}

ip_list = []

# 选取生成数据的路径
# path_dif_scale_data = "random_data_1bw"  #1百万数据量，每个文件大小为0.12G，共2.4G；
# path_dif_scale_data = "random_data_1qw"  #一千万数据量，每个文件大小为1.24G，共24G；
path_dif_scale_data = "random_data_2bw"

# 建立连接
client = InsecureClient(url='http://10.193.7.141:50070;http://10.193.7.21:50070', user="hdfs")
path_centreids_vector = "/region01/29962/app/develop/faiss_distribute_test/centroids_vector/part-00000-ccda3c40-e401-416e-beb5-955fbe6068df-c000.csv"
path_adid_index = "/region01/29962/app/develop/faiss_distribute_test/adid_index/part-00000-ada8da91-4504-435c-851a-36080e04d558-c000.csv"

dict_centreids_vector = get_centreids_data(client, path_centreids_vector)
dict_adid_index = get_adid_index_data(client, path_adid_index)

# 路径
path_list = []
path_list_dir = client.list('/region01/29962/app/develop/faiss_distribute_test/' + path_dif_scale_data)
path_list_dir.remove('_SUCCESS')
for i in range(len(path_list_dir)):
    path_list.append(
        "/region01/29962/app/develop/faiss_distribute_test/" + path_dif_scale_data + "/" + path_list_dir[i])
print(path_list)

# 映射路径和聚类中心
for centreid in range(len(path_list)):
    path_centreid_dict[path_list[centreid]] = centreid


@app.route("/register/", methods=['GET'])
def register():
    ipaddress = request.remote_addr

    if ipaddress not in ip_list:
        ip_list.append(ipaddress)
    len_path_list = len(path_list)
    # 初次注册
    if len(ip_list) <= len_path_list:
        index_path = ip_list.index(ipaddress)
        path = path_list[index_path]
        # 容器ip和拉取路径的映射
        ip_path_dict[ipaddress] = path
        path_ip_dict[path] = ipaddress

        # 获取IP和centreid的映射
        centreid = path_centreid_dict[path]
        ip_centreid_dict[ipaddress] = centreid
        centreid_ip_dict[centreid] = ipaddress

    # 有节点挂后，继续注册，注册的ip列表长度变长，
    # 先找到挂掉节点对应的path，并更改IP—path—cenreid对应的路径，修改参数
    # 找到发生修改的映射，重新映射IP
    elif len(ip_list) > len_path_list:
        for key in ip_path_dict:
            try:
                check_status = requests.get("http://%s:5051/check_slave/" % key)
            except Exception as e:
                # 将聚类中心对应的ip改为空，把路径对用的ip改为空
                path_ip_con = ip_path_dict[key]
                path_ip_dict[path_ip_con] = ""

                # 映射的ip和centreid
                centreid_ip_con = ip_centreid_dict[key]
                centreid_ip_dict[centreid_ip_con] = ""

        for item in path_ip_dict:
            if path_ip_dict[item] == "":
                path = item
                # 更改ip和路径之间的映射
                path_ip_dict[path] = ipaddress
                # 更改聚类中心和ip之间的映射
                centreid = path_centreid_dict[path]
                centreid_ip_dict[centreid] = ipaddress

    result_data = {}
    result_data["status"] = "success"
    result_data["path"] = path
    json_path = json.dumps(result_data)
    return json_path


@app.route("/faiss_service_master/", methods=['GET'])
def master_service():
    try:
        print(centreid_ip_dict)
        # app.logger.info(centreid_ip_dict)

        start_all_service_time = time.time()
        # 解析请求体,得到待查询向量
        start_query_analysis_time = time.time()
        nprobe_input = request.args.get('nprobe', '1')
        top_n_input = request.args.get("top_n")
        data_input = request.args.get("data")
        data_string = data_input
        topk = int(top_n_input)
        nprobe = int(nprobe_input)
        end_query_analysis_time = time.time()
        app.logger.info(
            "请求体解析成功，获得待查询数据，nprobe和topk,所用时间,logger：" + str(end_query_analysis_time - start_query_analysis_time))
        print("请求体解析成功，获得待查询数据，nprobe和topk,所用时间,logger：" + str(end_query_analysis_time - start_query_analysis_time))

        start_get_dif_query_data_time = time.time()
        data_list = data_string.split(",")
        xq = np.array(data_list).astype('float32')
        end_get_dif_query_data_time = time.time()
        app.logger.info(
            "获取所有的请求数据，并生成二维数组成功，所用时间logger：" + str(end_get_dif_query_data_time - start_get_dif_query_data_time))
        print("获取所有的请求数据，并生成二维数组成功，所用时间logger：" + str(end_get_dif_query_data_time - start_get_dif_query_data_time))

        start_calculate_distance_time = time.time()
        dict_dis = {}
        # 计算待查询向量与各个质心的距离
        for key in dict_centreids_vector:
            centre_array = np.array(dict_centreids_vector[key])
            distance = np.linalg.norm(xq - centre_array)
            dict_dis[key] = distance
        end_calculate_distance_time = time.time()
        app.logger.info(
            "计算待查询向量与聚类中心的距离成功，所用时间,logger：" + str(end_calculate_distance_time - start_calculate_distance_time))
        print("计算待查询向量与聚类中心的距离成功，所用时间,logger：" + str(end_calculate_distance_time - start_calculate_distance_time))

        # 根据距离排序,并返回前nprobe个质心
        start_sort_by_dis_time = time.time()
        list_dis_sorted = sorted(dict_dis.items(), key=lambda d: d[1])[0:nprobe]
        end_sort_by_dis_time = time.time()
        app.logger.info("排序成功，所用时间：" + str(end_sort_by_dis_time - start_sort_by_dis_time))
        print("排序成功，所用时间：" + str(end_sort_by_dis_time - start_sort_by_dis_time))

        for item in list_dis_sorted:
            app.logger.info(str(item))
            print(str(item))

        for item in centreid_ip_dict:
            app.logger.info(str(item))
            app.logger.info(str(centreid_ip_dict[item]))
            print(str(item))
            print(str(centreid_ip_dict[item]))


        # 将待查询向量发送给相应slave,找到对应的从节点，然后将xq通过http发送出去,
        # 获取slave返回结果，并将所有结果添加到数组中
        D_I_all_slave = []
        start_query_slave_time = time.time()
        for item in list_dis_sorted:
            # item为质心的id
            slave_ip = centreid_ip_dict[item[0]]
            try:
                slave_return_json = requests.get(
                    "http://%s:5051/faiss_service/?top_n=20&data=%s" % (slave_ip, data_string))
                D_I_list_all_slave = json.loads(slave_return_json.text)['results']
                app.logger.info("requests 响应时间：" + str(slave_return_json.elapsed.microseconds))
                print("requests 响应时间：" + str(slave_return_json.elapsed.microseconds))

                app.logger.info(str(D_I_list_all_slave))
                print(str(D_I_list_all_slave))
            except requests.ConnectionError as e:
                app.logger.info(e)
                print(e)

            # D_I_list_all_slave = [[["6.6989465", "29088"], ["6.9235916", "8123"], ["6.9933767", "7843"], ["7.016519", "55215"]]]
            start_add_slave_result_data_time = time.time()
            for list_each_dim in D_I_list_all_slave:
                for list in list_each_dim:
                    length_list = len(list)
                    if length_list == 2:
                        distance = float(list[0])
                        adid_index = list[1]
                        D_I_all_slave.append((adid_index, distance))
            end_add_slave_result_data_time = time.time()
            app.logger.info(
                "add slave data time::" + str(end_add_slave_result_data_time - start_add_slave_result_data_time))
            print("add slave data time::" + str(end_add_slave_result_data_time - start_add_slave_result_data_time))
        end_query_slave_time = time.time()
        app.logger.info(
            "获取从节点信息，并且发送请求给从节点，并接收返回信息存储为list,所用时间,logger：" + str(end_query_slave_time - start_query_slave_time))
        print("获取从节点信息，并且发送请求给从节点，并接收返回信息存储为list,所用时间,logger：" + str(end_query_slave_time - start_query_slave_time))

        # 重排序
        start_resort_time = time.time()
        result_all_sort_topk = sorted(D_I_all_slave, key=lambda d: d[1])[0:topk]
        end_resort_time = time.time()
        app.logger.info("重排序成功，所用时间：" + str(end_resort_time - start_resort_time))
        print("重排序成功，所用时间：" + str(end_resort_time - start_resort_time))

        # 取出adid_index
        start_get_adid_index = time.time()
        result_evequery_adid_index = []
        for i in range(topk):
            result_evequery_adid_index.append(result_all_sort_topk[i][0])
        end_get_adid_index = time.time()
        app.logger.info("返回adid_index:" + str(end_resort_time - start_resort_time))
        print("返回adid_index:" + str(end_resort_time - start_resort_time))

        # 查找adid的映射并返回
        start_get_adid = time.time()
        adid_return_evequery = []
        for adid_index in result_evequery_adid_index:
            adid = dict_adid_index[adid_index]
            adid_return_evequery.append(adid)
        end_get_adid = time.time()
        app.logger.info("返回adid:" + str(end_get_adid - start_get_adid))
        print("返回adid:" + str(end_get_adid - start_get_adid))

        # 返回结果，包括状态和adid列表
        start_data_return_to_user = time.time()
        result_data = {}
        result_data["status"] = "success"
        key = "results"
        result_data[key] = adid_return_evequery
        result_json = json.dumps(result_data)
        end_data_return_to_user = time.time()
        app.logger.info("生成返回数据" + str(end_data_return_to_user - start_data_return_to_user))
        print("生成返回数据" + str(end_data_return_to_user - start_data_return_to_user))

        end_all_service_time = time.time()
        app.logger.info("************total time*********" + str(end_all_service_time - start_all_service_time))
        print("************total time*********" + str(end_all_service_time - start_all_service_time))
        return result_json
    except (AssertionError, ValueError) as e:
        print(e)
        return str(e)
    except Exception as e2:
        print(e2)
        return str(e2)


@app.route("/exit_flask/")
def master_exit():
    os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
    os._exit(0)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5051, debug=True, use_reloader=False)