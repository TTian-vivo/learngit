import hdfs
import requests
import json
import csv

path='/region04/27141/app/develop/11103914/sl_data'
url='http://10.193.7.158:50070;http://10.193.7.28:50070'
Client = hdfs.InsecureClient(url=url, user='hdfs')

#从hdfs读取数据
all_data_str = ""
for dirpath, dirnames, filenames in Client.walk(path):
    for filepath in filenames:
        print(dirpath + '/' + filepath)
        with Client.read(dirpath + '/' + filepath) as reader:
            all_data_hdfs = reader.read()
            data_str = bytes.decode(all_data_hdfs)
            all_data_str = all_data_str + data_str

#数据读取后，做数据处理，处理为adid和vector的格式
adids_vectors = all_data_str.split("\n")

for adid_vector_tmp in adids_vectors:
    adid_vector = adid_vector_tmp.split(",")
    adid = adid_vector[0]
    vector_list_str=adid_vector[1:]
    vector = []
    for i in vector_list_str:
        vector.append(float(i))
    url = "http://10.193.65.45:9001/test_data_num_vearch/sl_data/{}".format(adid)
    body = {'adid': adid, "vector": {"feature": vector}}
    response = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    print(json.loads(response.text)["status"])
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
