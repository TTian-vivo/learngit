import hdfs
import requests
import json


path='/region02/32716/app/develop/recall/pic_vector/pic_vector_result.txt'
url='http://10.193.7.152:50070;http://10.193.7.24:50070'
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
adids_vectors_ = all_data_str.split("\n")
adids_vectors= [x for x in adids_vectors_ if x != '']

for adid_vector_tmp in adids_vectors:
    adid_vector = adid_vector_tmp.split(",")
    if len(adid_vector)==129:
        adid = adid_vector[0]
        vector_list_str=adid_vector[1:]
        vector = []
        for i in vector_list_str:
            vector.append(float(i))
        url1 = "http://10.192.23.145:9001/multi_mode_mech_test/yw_data_p1/{}".format(adid)
        #url2 = "http://10.192.23.145:9001/multi_mode_mech_test/yw_data_p2/{}".format(adid)
        body = {'adid': adid, "vector": {"feature": vector}}
        response1 = requests.post(url1, headers={"content-type": "application/json",'Connection': 'close'}, data=json.dumps(body))
        #response2 = requests.post(url2, headers={"content-type": "application/json"}, data=json.dumps(body))
        try:
            status1 = json.loads(response1.text)["status"]
            #status2 = json.loads(response2.text)["status"]
            if status1 == 201:
                print("data insert is ok")
            else:
                # 此处应该抛出异常
                print("{} is not insert success".format(adid))
        except Exception:
            # 此处应该捕获异常，并作出处理
            print("insert data error!")
