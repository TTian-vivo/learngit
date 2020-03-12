import time
from milvus import Milvus, IndexType, MetricType
from flask import Flask, request
import json

now = time.strftime("%Y-%m-%d", time.localtime())
app = Flask(__name__)

table_name = 'examples_milvus'
server_config = {
    "host": '10.193.65.45',
    "port": '19530',
}
milvus = Milvus()
milvus.connect()

_DIM = 300

def item_vectors():
    path_item_vector = "/root/ttian/feng_milvus/nce_weights"
    item_vector = []
    with open(path_item_vector, 'r') as f:
        item_data_string = f.read()
        item_data_list = item_data_string.split("\n")
        for each_item_string in item_data_list:
            if each_item_string != "":
                each_item_list = each_item_string.split(",")
                item_vector.append(list(map(float, each_item_list)))
    return item_vector

def create_table():
    param = {
        'table_name': table_name,
        'dimension': _DIM,
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

def build_index():
    print("Start build index ...... ")
    index = {
        "index_type": IndexType.IVFLAT,
        "nlist": 128
    }
    status = milvus.create_index(table_name, index)
    if not status.OK():
        print("build index failed: {}".format(status.message))
    else:
        raise RuntimeError("Build index failed")

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
        'nlist': 128
    }
    print("starting create index ... ")
    milvus.create_index(table_name, _index)
    time.sleep(2)

print("start get data")
vectors = item_vectors()
print("get data ready")

#建表
create_table()

#describe_table()
insert_vectors(vectors)

# wait for data persisting
time.sleep(2)

# create index
create_index()

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
        status,results = search_vectors(query_vector, nprobe, top_K)
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
    app.run(host='0.0.0.0', port=5051, use_reloader=False)