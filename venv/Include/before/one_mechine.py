# import numpy as np
# import faiss
# import time
# import os
# import json
# import hdfs
# import gc
# from flask import Flask,request
# #
# # now = time.strftime("%Y-%m-%d", time.localtime())  # 获取现在的日期
# # app = Flask(__name__)
#
#
# # @app.route("/faiss_service_master/", methods=['GET'])
# # def pku_seg1():
# #     try:
# #         nprobe_input = request.args.get("nprobe")
# #         top_n_input = request.args.get("top_n")
# #         data_input = request.args.get("data")
# #         data_string = data_input.split(",")
# #         data_list = []
# #         for i in range(len(data_string)):
# #             data_list.append(float(data_string[i]))
# #
# #         k = int(top_n_input)
# #         index.nprobe = int(nprobe_input)
# #         xq = np.array([data_list]).astype('float32')
# #
# #         result_list = []
# #         D,I = index.search(xq, k)
# #         for j in range(len(I)):
# #             D_str = [str(val) for val in D[j]]
# #             I_str = [str(val) for val in I[j]]
# #             D_I = list(zip(D_str,I_str))
# #             result_list.append(D_I)
# #
# #         result_data = {}
# #         result_data["status"] = "success"
# #         result_data["results"] = result_list
# #         result_json = json.dumps(result_data)
# #         return result_json
# #
# #     except (AssertionError):
# #         return "input error!!! 传入的维数不对"
# #     except ValueError:
# #         return "input error!!! 传参方式错误"
# #
# # @app.route("/exit_flask/")
# # def pku_seg2():
# #     os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
# #     os._exit(0)
#
# if __name__ == '__main__':
#
#     #client = hdfs.InsecureClient(url='http://10.21.52.65:50070;http://10.20.94.70:50070', user='hdfs')
#     url_tmp = 'http://10.21.52.65:50070;http://10.20.94.70:50070'
#     client = hdfs.InsecureClient(url=url_tmp, user='hdfs')
#
#     path_list = []
#     path_list_dir = client.list('/region4/29297/app/develop/faiss_distribute/random_data_1bw')
#     path_list_dir.remove('_SUCCESS')
#     for i in range(len(path_list_dir)):
#         path_list.append("/region4/29297/app/develop/faiss_distribute/random_data_less" + "/" + path_list_dir[i])
#     print(path_list)
#
#
#     data_list = []
#     index_list = []
#
#     for file_path in path_list:
#         with client.read(file_path) as reader:
#             data_hdfs = reader.read()
#             data_str = bytes.decode(data_hdfs).replace("\"[", "").replace("]\"", "")
#             data_split = data_str.split("\n")
#             for line in data_split:
#                 if line != "":
#                     line_split = line.split(",")
#                     data_str_tmp = line_split[3:]
#                     index_temp = line_split[2]
#                     data_list.append(data_str_tmp)
#                     index_list.append(index_temp)
#             del data_hdfs
#             del data_str
#             del data_split
#             gc.collect()
#
#     for i in range(len(path_list)):
#         path = path_list[i]
#         with client.read(path) as reader:
#             data = reader.read
#
#
#     xb = np.array(data_list).astype('float32')
#     ids = np.array(index_list).astype('int')
#     d = 128
#     start_create_index_time = time.time()
#     nlist = 20
#     index1 = faiss.IndexFlatL2(d)
#     index = faiss.IndexIVFFlat(index1, d, nlist)
#     index.train(xb)
#     end_create_index_time = time.time()
#     print("the index create time is :::" , end_create_index_time - start_create_index_time)
#
#     start_add_data_time = time.time()
#     index.add(xb)
#     end_add_data_time = time.time()
#     print("the add data time is :::",end_add_data_time-start_add_data_time)
#
#     #app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
#
#     xq = np.array([[0.61,0.46,0.97,0.82,0.38,0.58,0.38,0.15,0.65,0.42,0.5,0.63,0.87,0.58,0.35,0.03,0.56,0.44,0.12,0.4,0.71,0.5,0.22,0.19,0.3,0.65,0.46,0.75,0.08,0.27,0.02,0.15,0.19,0.35,0.9,0.85,1.,0.08,0.91,0.85,0.39,0.97,0.04,0.03,0.47,0.99,0.64,0.2,0.58,0.44,0.2,0.22,0.86,0.63,0.12,0.18,0.42,0.83,0.72,0.67,0.33,0.11,0.47,0.97,0.15,0.54,0.15,0.69,0.68,0.86,0.62,0.07,0.52,0.96,0.05,0.41,0.23,0.13,0.7,0.75,0.45,0.28,0.37,0.88,0.17,0.89,0.08,0.73,0.13,0.28,0.27,0.12,0.81,0.37,0.65,0.71,0.54,0.64,0.45,0.54,0.54,0.77,0.39,0.26,0.43,0.43,0.41,0.74,0.32,0.39,0.48,0.14,0.55,0.81,0.43,0.02,0.73,0.74,0.33,0.03,0.37,0.38,0.16,0.2,0.5,0.28,0.1,0.88]]).astype('float32')
#
#     start_search_time = time.time()
#     D,I = index.search(xq, 20)
#     end_search_time = time.time()
#     print("***************************************")
#     print("***************************************")
#     print("***************************************")
#     print("***************************************")
#     print("查询时间为：",end_search_time-start_search_time)
#
#     start_add_data_time = time.time()
#     result_list=[]
#     for j in range(len(I)):
#         D_str = [str(val) for val in D[j]]
#         I_str = [str(val) for val in I[j]]
#         D_I = list(zip(D_str, I_str))
#         result_list.append(D_I)
#     end_add_data_time = time.time()
#     print("从节点数据添加时间：" + str(end_add_data_time - start_add_data_time))
#
#     # start_my_method_time = time.time()
#     # D_I_couple = []
#     # for i in range(len(I)):
#     #     D_I_couple.append((str(D[0][i]),str(I[0][i])))
#     # end_my_method_time = time.time()
#     # print("我的方法的数据添加时间：",end_my_method_time-start_my_method_time)

import random
a = {1:'a',2:'b',3:'c'}

b = random.choice(list(a.values()))
print(b)