import numpy as np
import faiss
import time
import os
import json
import hdfs
import gc
from flask import Flask,request

now = time.strftime("%Y-%m-%d", time.localtime())
app = Flask(__name__)

url_tmp = 'http://10.193.7.141:50070;http://10.193.7.21:50070'
client = hdfs.InsecureClient(url=url_tmp, user='hdfs')

path_dif_scale_data = "random_data_2bw"

path_list = []
path_list_dir = client.list('/region01/29962/app/develop/faiss_distribute_test/' + path_dif_scale_data)
path_list_dir.remove('_SUCCESS')
for i in range(len(path_list_dir)):
    path_list.append("/region01/29962/app/develop/faiss_distribute_test/"+path_dif_scale_data+"/"+path_list_dir[i])
print(path_list)


data_list = []
index_list = []

for file_path in path_list:
    with client.read(file_path) as reader:
        data_hdfs = reader.read()
        data_str = bytes.decode(data_hdfs).replace("\"[", "").replace("]\"", "")
        data_split = data_str.split("\n")
        for line in data_split:
            if line != "":
                line_split = line.split(",")
                data_str_tmp = line_split[3:]
                index_temp = line_split[2]
                data_list.append(data_str_tmp)
                index_list.append(index_temp)
        del data_hdfs
        del data_str
        del data_split
        gc.collect()

xb = np.array(data_list).astype('float32')
ids = np.array(index_list).astype('int')
d = 128
start_create_index_time = time.time()
nlist = 20
index1 = faiss.IndexFlatL2(d)
index = faiss.IndexIVFFlat(index1, d, nlist)
index.train(xb)
end_create_index_time = time.time()
print("the index create time is :::" , end_create_index_time - start_create_index_time)

start_add_data_time = time.time()
index.add(xb)
end_add_data_time = time.time()
print("the add data time is :::",end_add_data_time-start_add_data_time)

del xb
del ids
gc.collect()

@app.route("/faiss_service_master/", methods=['GET'])
def pku_seg1():
    try:
        nprobe_input = request.args.get("nprobe")
        top_n_input = request.args.get("top_n")
        data_input = request.args.get("data")
        data_string = data_input.split(",")
        data_list = []
        for i in range(len(data_string)):
            data_list.append(float(data_string[i]))

        k = int(top_n_input)
        index.nprobe = int(nprobe_input)
        xq = np.array([data_list]).astype('float32')

        result_list = []
        start_search_time = time.time()
        D,I = index.search(xq, 20)
        end_search_time = time.time()
        print("***************************************")
        print("***************************************")
        print("查询时间为：" + str(end_search_time-start_search_time))

        start_my_method_time = time.time()
        D_I_couple = []
        for i in range(len(I[0])):
            D_I_couple.append((str(D[0][i]),str(I[0][i])))
        end_my_method_time = time.time()
        print("我的方法的数据添加时间："+str(end_my_method_time - start_my_method_time))

        result_data = {}
        result_data["status"] = "success"
        result_data["results"] = D_I_couple
        result_json = json.dumps(result_data)
        return result_json

    except (AssertionError):
        return "input error!!! 传入的维数不对"
    except ValueError:
        return "input error!!! 传参方式错误"

@app.route("/exit_flask/")
def pku_seg2():
    os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
    os._exit(0)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)




