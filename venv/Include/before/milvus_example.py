import random
import time
from milvus import Milvus, IndexType, MetricType

_DIM = 128
nb = 1000000  # number of vector dataset
nq = 100  # number of query vector
table_name = 'examples_milvus'
partition_tag = "random_data1"
top_K = 10

server_config = {
    "host": '10.193.65.45',
    "port": '19530',
}

milvus = Milvus()
milvus.connect()


def random_vectors(num):
    # generate vectors randomly
    return [[random.random() for _ in range(_DIM)] for _ in range(num)]

def create_table():
    param = {
        'table_name': table_name,
        'dimension': _DIM,
        #设置文件大小
        #'index_file_size': 4096,
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

def delete_partitions():
    #status = milvus.delete_partion('table_name':table_name, 'tag':"aaa")
    pass

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
        "nlist": 512
    }
    status = milvus.create_index(table_name, index)
    if not status.OK():
        print("build index failed: {}".format(status.message))
    else:
        raise RuntimeError("Build index failed")


def search_vectors(_query_vectors):
    """
    search vectors and display results
    :param _query_vectors:
    :return: None
    """
    status, results = milvus.search_vectors(table_name=table_name, query_records=_query_vectors, top_k=top_K,
                                            nprobe=16)
    #分区查询
    #milvus.search_vectors(table_name=table_name, query_records=_query_vectors, top_k=top_K, nprobe=16, partition_tags="aaa")
    print(type(results))
    print(results)
    if not status.OK():
        print("search failed. {}".format(status))
    else:
        print('serach successfully!')
        print("\nresult:\n{}".format(results))


def create_index():
    _index = {'index_type': IndexType.IVFLAT,'nlist': 128}
    print("starting create index ... ")
    milvus.create_index(table_name, _index)
    time.sleep(2)

def create_partition(partition_name,partition_tag):
    status = milvus.create_partition(table_name=table_name, partition_name=partition_name, partition_tag=partition_tag)
    if status.OK:
        print("create partition %s success!"%partition_name)
    else:
        print("create partition %s failed!"%partition_name)

def insert_vectors_partition(_vectors,partition_tag):
    """
    insert vectors to milvus server
    :param _vectors: list of vector to insert
    :return: None
    """
    print("Starting insert vectors to partition ... ")
    status, ids = milvus.add_vectors(table_name=table_name, records=_vectors, partition_tag=partition_tag)
    if not status.OK():
        print("insert failed: {}".format(status.message))
    status, count = milvus.get_table_row_count(table_name)
    if status.OK():
        print("insert vectors into table `{}` successfully!".format(table_name))
    else:
        raise RuntimeError("Insert error")

def search_vectors_partition(query_vectors,partition_tag):
    status, results = milvus.search_vectors(table_name=table_name, query_records=query_vectors, top_k=top_K,
                                            nprobe=16,partition_tags=[partition_tag])
    print(results.id_array)
def run():
    vectors = random_vectors(nb)

    #create_table()
    create_partition("partition_1",partition_tag)

    #describe_table()

    #insert_vectors(vectors)
    insert_vectors_partition(vectors,partition_tag)

    # wait for data persisting
    time.sleep(2)

    # create index
    create_index()

    #milvus.describe_table(table_name)

    #status, index = milvus.describe_index(table_name)
    #print(index)

    # generate query vectors

    query_vectors = random_vectors(nq)
    print(type(query_vectors))
    print(query_vectors)

    t0 = time.time()
    search_vectors_partition(query_vectors,partition_tag)
    print("Cost {:.4f} s".format(time.time() - t0))

    # delete index
    #milvus.drop_index(table_name=table_name)

    #time.sleep(3)

    # delete table
    #delete_table()
    #milvus.show_partitions(table_name=table_name)
    #add_vector(table_name=table_name, records=vec_list, ids=vec_ids, partition_tag="random_data")

if __name__ == '__main__':
    run()