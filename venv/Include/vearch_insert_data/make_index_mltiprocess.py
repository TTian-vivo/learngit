import hdfs
import requests
import json
import time
import multiprocessing


def get_data():
    ##取数据的过程
    # path='/region05/29962/app/develop/recall_alg'
    # url='http://10.193.7.159:50070;http://10.193.7.35:50070'
    path = '/region02/32716/app/develop/recall/pic_vector/pic_vector_result.txt'
    url = 'http://10.193.7.152:50070;http://10.193.7.24:50070'
    Client = hdfs.InsecureClient(url=url, user='hdfs')
    all_data_str = ""
    for dirpath, dirnames, filenames in Client.walk(path):
        for filepath in filenames:
            print(dirpath + '/' + filepath)
            with Client.read(dirpath + '/' + filepath) as reader:
                all_data_hdfs = reader.read()
                data_str = bytes.decode(all_data_hdfs)
                all_data_str = all_data_str + data_str

    #数据读取后，做数据处理，处理为adid和vector的格式
    adids_vectors_ = all_data_str.split("\n")
    global adids_vectors
    adids_vectors= [x for x in adids_vectors_ if x != '']
    #len(adids_vectors)##获取数据量

def post(adid,vector):
    #服务端口和IP
    url = "http://10.192.23.145:9001/multi_mode_mech_test/yw_data_p2/{}".format(adid)
    #请求体
    body = {'adid': adid, "vector": {"feature": vector}}
    #请求发送
    response = requests.post(url, headers={"content-type": "application/json",'Connection': 'close'}, data=json.dumps(body))
    #结果检验，是否插入成功
    try:
        status = json.loads(response.text)["status"]
        if status == 201:
            print("data insert is ok")
        else:
            # 此处应该抛出异常
            print("{} is not insert success".format(adid))
    except Exception:
        # 此处应该捕获异常，并作出处理
        print("insert data error!")



if __name__ == '__main__':
    get_data()
    start=time.time()
    pool = multiprocessing.Pool(processes=64)
    ##现在是向服务中插入数据
    for adid_vector_tmp in adids_vectors:
        adid_vector = adid_vector_tmp.split(",")
        if len(adid_vector) == 129:
            adid = adid_vector[0]
            vector_list_str = adid_vector[1:]
            vector = []
            for i in vector_list_str:
                vector.append(float(i))
        pool.apply_async(post, (adid,vector))
    pool.close()
    pool.join()
    end=time.time()
    print(end-start)
    print("insert data is over！")
