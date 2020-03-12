import time
from milvus import Milvus, IndexType, MetricType
import json
from flask import Flask, request
import hdfs
import gc
import numpy as np

now = time.strftime("%Y-%m-%d", time.localtime())
app = Flask(__name__)

table_name = 'examples_milvus'
server_config = {
    "host": '10.193.65.45',
    "port": '19530',
}
DIM =128
milvus = Milvus()
milvus.connect()

def one_million_vectors():
    url_tmp = 'http://10.193.7.141:50070;http://10.193.7.21:50070'
    client = hdfs.InsecureClient(url=url_tmp, user='hdfs')

    path_dif_scale_data = "random_data_2bw"

    path_list = []
    path_list_dir = client.list('/region01/29962/app/develop/faiss_distribute_test/' + path_dif_scale_data)
    path_list_dir.remove('_SUCCESS')
    for i in range(len(path_list_dir)):
        path_list.append("/region01/29962/app/develop/faiss_distribute_test/" + path_dif_scale_data + "/" + path_list_dir[i])
    print(path_list)

    data_list = []

    for file_path in path_list:
        with client.read(file_path) as reader:
            data_hdfs = reader.read()
            data_str = bytes.decode(data_hdfs).replace("\"[", "").replace("]\"", "")
            data_split = data_str.split("\n")
            for line in data_split:
                if line != "":
                    line_split = line.split(",")
                    data_str_tmp = line_split[3:]
                    #index_temp = line_split[2]
                    data_list.append(data_str_tmp)
                    #index_list.append(index_temp)
            del data_hdfs
            del data_str
            del data_split
            gc.collect()
    vectors = np.array(data_list).astype('float32').tolist()
    return vectors

def create_table():
    param = {
        'table_name': table_name,
        'dimension': DIM,
        'index_file_size': 1024,
        'metric_type': MetricType.L2
    }
    if milvus.has_table(table_name):
        milvus.delete_table(table_name)
        time.sleep(2)
    print("Create table: {}".format(param))
    status = milvus.create_table(param)
    if status.OK():
        print("create table {} successfully!".format(table_name))
    else:
        print("create table {} failed: {}".format(table_name, status.message))

def delete_table():
    status = milvus.delete_table(table_name)
    if status.OK():
        print("table {} delete successfully!".format(table_name))
    else:
        print("table {} delete failed: {}".format(table_name, status.message))

def describe_table():
    """
    Get schema of table
    :return: None
    """
    status, schema = milvus.describe_table(table_name, timeout=1000)
    if status.OK():
        print('Describing table `{}` ... :\n'.format(table_name))
        print('{}'.format(schema))
    else:
        print("describe table failed: {}".format(status.message))

def insert_vectors(_vectors):
    """
    insert vectors to milvus server
    :param _vectors: list of vector to insert
    :return: None
    """
    print("Starting insert vectors ... ")
    status, ids = milvus.add_vectors(table_name=table_name, records=_vectors)
    if not status.OK():
        print("insert failed: {}".format(status.message))
    status, count = milvus.get_table_row_count(table_name)
    if status.OK():
        print("insert vectors into table `{}` successfully!".format(table_name))
    else:
        raise RuntimeError("Insert error")
# def build_index():
#     print("Start build index ...... ")
#     index = {
#         "index_type": IndexType.IVFLAT,
#         "nlist": 4096
#     }
#     status = milvus.create_index(table_name, index)
#     if not status.OK():
#         print("build index failed: {}".format(status.message))
#     else:
#         raise RuntimeError("Build index failed")
def search_vectors(_query_vectors,nprobe,top_K):
    """
    search vectors and display results
    :param _query_vectors:
    :return: None
    """
    status, results = milvus.search_vectors(table_name=table_name, query_records=_query_vectors, top_k=top_K,
                                            nprobe=nprobe)
    return status,results

def create_index():
    _index = {
        'index_type': IndexType.IVFLAT,
        'nlist': 1024
    }
    print("starting create index ... ")
    milvus.create_index(table_name, _index)
    time.sleep(2)

start_add_data_time = time.time()
vectors = one_million_vectors()
end_add_data_time = time.time()
print("data time is ::"+str(end_add_data_time-start_add_data_time))

#建表
start_create_table_time= time.time()
create_table()
end_create_table_time = time.time()
print("create table time:" + str(end_create_table_time-start_create_table_time))

start_insert_time = time.time()
insert_vectors(vectors)
end_insert_time = time.time()
print("insert time is:" + str(end_insert_time-start_insert_time))

# wait for data persisting
time.sleep(2)

# create index
start_create_index_time= time.time()
create_index()
end_create_index_time = time.time()
print("create index time is ::" + str(end_create_index_time-start_create_index_time))


milvus.describe_table(table_name)

status, index = milvus.describe_index(table_name)
print(index)

@app.route("/milvus_service/", methods=['GET'])
def milvus_service():
    try:
        print("start analysis")
        nprobe = int(request.args.get("nprobe"))
        top_K = int(request.args.get("top_n"))
        data_string = request.args.get("data")
        data_string_split = data_string.split(",")
        print("end analysis")

        print("start add data")
        query_data = []
        for i in range(len(data_string_split)):
            query_data.append(float(data_string_split[i]))
        print("end add data")

        print("start search vector")
        query_vector = [query_data]
        status, results = search_vectors(query_vector, nprobe, top_K)
        print(type(results))

        if not status.OK():
            status_info = "search failed. {}".format(status)
            print(status_info)
            results_return = "failed"
        else:
            # app.logger.info('serach successfully!')
            # app.logger.info("\nresult:\n{}".format(results))
            print('serach successfully!')
            results_return = str(results)

        result_data = {}
        result_data["status"] = "success"
        result_data["results"] = results_return
        result_json = json.dumps(result_data)
        return result_json

    except AssertionError as e1:
        return e1
    except ValueError as e2:
        return e2

@app.route("/milvus_exit/", methods=['GET'])
def milvus_exit():
    # delete index
    milvus.drop_index(table_name=table_name)
    time.sleep(3)
    # delete table
    delete_table()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, use_reloader=False)