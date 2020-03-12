import hdfs
import requests
import json
"""
使用方向1：建立数据库

curl -XPUT -H "content-type:application/json" -d '{
    "name": "db_name"——（数据库名,需要自己定义）
}
' http://10.102.23.108:8817（建立数据库需要访问的ＩＰ和端口，目前不需要更改）/db/_create


使用方向２：建立空间（数据库的下一级，所有的ｉｎｄｅｘ的信息会在这里定义）
curl -XPUT -H "content-type: application/json" -d'
{
    "name": "space1",（空间名，自定义）
    "partition_num": 1,（分区个数————————自己定义，目前有两台用作查询的机器，因此定为２性能最好）
    "replica_num": 1,（副本个数——保证数据安全）
    "engine": {
        "name": "gamma",
        "index_size": 70000,————数据库里的数据达到多少，开始建立索引，可以定个小一些的数字
        "max_size":2000000,
        "nprobe": 10,————————查询时ｎｐｒｏｂｅ——决定精度
        "metric_type": "InnerProduct",——InnerProduct和Ｌ２两种，Ｌ２是ｃｏｓ
        "ncentroids": 256,——聚类个数
        "nsubvector": 32——PQ量化值
    },
    "properties": {
        "ａｄｉｄ": {
            "type": "string",
            "array": true,
            "index": true
        },
        "ｖｅｃｔｏｒ": {
            "type": "vector",
            "dimension": 128
        }
    }
}
' http://10.102.23.108:8817（建立空间需要访问的ＩＰ和端口，目前不需要更改）/space/$db_name（你自己定义的数据库名称）/_create
"""

"""
应用方向3：插入数据，此处的例子为直接把hdfs的数据全量插入库中
"""
##取数据的过程
path='/region05/29962/app/develop/recall_alg'
url='http://10.193.7.159:50070;http://10.193.7.35:50070'
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

len(adids_vectors)##获取数据量
##现在是向服务中插入数据
for adid_vector_tmp in adids_vectors:
    adid_vector = adid_vector_tmp.split("\t")
    adid = adid_vector[0]
    vector_str = adid_vector[1]
    vector_list_str = vector_str.split(',')
    vector = []
    for i in vector_list_str:
        vector.append(float(i))

    #服务端口和IP
    url = "http://10.192.23.145:9001/multi_mode_mech_test/recall_alg_p2/{}".format(adid)

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


"""
应用方向4：查询数据
"""
url = "http://10.192.23.145:9001(查询的IP和端口，目前不需要改变)/multi_mode_test（数据库，要自己定义）/recall_alg_p1（空间，要自己定义）/_search"
body = {"query": {"sum": [{"field": "vector","feature": [-0.055293478, -0.064293445, -0.028965434, 0.035174932, -0.05665691, -0.11934104, -0.148104, -0.13718222, 0.06421166, 0.15961613, -0.08074932, 0.078490034, 0.0615455, -0.2441095, 0.21817107, 0.034447342, 0.07793621, -0.04868924, 0.08324438, 0.01379166, -0.07940866, -0.052381676, 0.077610515, 0.06860451, -0.13166705, 0.052400466, -0.08665852, 0.058538586, -0.044768985, -0.15160485, 0.039664555, -0.11828959, -0.08408942, 0.010006074, -0.054367103, -0.05128289, -0.087664366, -0.013216431, -0.10461231, -0.088638484, 0.15488677, -0.03680268, 0.09397271, -0.0066388496, 0.0144284535, 0.015362044, -0.100107424, 0.13130528, 0.024602396, 0.04509604, 0.026031155, -0.10248218, 0.063076615, -0.011486272, -0.065867506, 0.03606617, 0.08935558, -0.19194455, -0.1462848, 0.21527843, -0.026132176, 0.014034832, -0.01608249, 0.10894165, -0.12771578, -0.019117026, 0.103432894, 0.27576172, -0.23912969, -0.0931059, 0.08398303, -0.052887958, 0.14136682, 0.040660996, -0.18404266, -0.00029697243, 0.021293294, 0.04603236, 0.15152417, -0.07875296, 0.085237615, 0.1552371, -0.018358244, -0.04661129, -0.057693034, 0.05717628, -0.008100048, 0.049104106, 0.14635317, -0.12147713, 0.024995798, 0.0076049897, 0.10092067, -0.030064348, -0.015831402, 0.067863286, -0.2885544, 0.031701025, 0.11083286, -0.007973804, -0.08976705, 0.120387934, -0.11179187, -0.1294898, 0.094342045, -0.04748301, -0.06318883, -0.045073453, -0.016764853, -0.013735498, 0.062274493, -0.13397337, -0.014897113, 0.09433972, 0.0197376, 0.01802045, -0.0037128457, -0.004229647, 0.013677028, -0.112296365, -0.0664894, 0.08679044, -0.08023114, -0.009391336, 0.11613684, -0.14699784, -0.12451736, 0.20240898] }]},"direct_search_type": 0,"quick": False,"vector_value": False,"online_log_level": "debug","size": 10}
response = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
resault = json.loads(response.text)

#全部查询，现在先不要用设置最大和最小分值的功能，目前还有些问题；
"""{
    "query": {
        "sum": [{
            "field": "field_name",
            "feature": [0.1, 0.2, 0.3, 0.4, 0.5],
            "min_score": 0.9,————先不要设置
            "max_score":1,————先不要设置
            "boost": 0.5
        }],
        "filter": [{
            "range": {
                "field_name": {
                    "gte": 160,
                    "lte": 180
                }
            }
        },
        {
             "term": {
                 "field_name": ["100", "200", "300"],
                 "operator": "or"
             }
        }]
    },
    "direct_search_type": 0,
    "quick": false,
    "vector_value": false,
    "online_log_level": "debug",
    "size": 10
}
' http://router_server/$db_name/$space_name/_search"""
"""
参数如下：
sum 支持多个(对应定义表结构时包含多个特征字段)。
field 指定创建表时特征字段的名称。
feature 传递特征，维数和定义表结构时维数必须相同。
min_score 指定返回结果中分值必须大于等于0.9，两个向量计算结果相似度在0-1之间，min_score可以指定返回结果分值最小值，max_score可以指定最大值。如设置： “min_score”: 0.8，“max_score”: 0.95 代表过滤0.8<= 分值<= 0.95 的结果。同时另外一种分值过滤的方式是使用: “symbol”:”>=”，”value”:0.9 这种组合方式，symbol支持的值类型包含: > 、 >= 、 <、 <= 4种，value及min_score、max_score值在0到1之间。
boost指定相似度的权重，比如两个向量相似度分值是0.7，boost设置成0.5之后,返回的结果中会将分值0.7乘以0.5即0.35。

ter 条件支持多个，多个条件之间是交的关系。
range 指定使用数值字段integer/float 过滤， filed_name是数值字段名称， gte、lte指定范围， lte 小于等于， gte大于等于，若使用等值过滤，lte和gte设置相同的值。上述示例表示查询field_name字段大于等于160小于等于180区间的值。
term 使用标签过滤， field_name是定义的标签字段，允许使用多个值过滤，可以求交“operator”: “or” , 求并: “operator”: “and”，上述示例表示查询field_name字段值是”100”、”200” 或”300”的值。
direct_search_type 指定查询类型，0代表若特征已经创建索引则使用索引，若没有创建则暴力搜索； -1 代表只使用索引进行搜索， 1代表不使用索引只进行暴力搜索。默认值是0。
quick 搜索结果默认将PQ召回向量进行计算和精排，为了加快服务端处理速度设置成true可以指定只召回，不做计算和精排。
vector_value为了减小网络开销，搜索结果中默认不包含特征数据只包含标量信息字段，设置成true指定返回结果中包含原始特征数据。
online_log_level 设置成”debug” 可以指定在服务端打印更加详细的日志，开发测试阶段方便排查问题。
size 指定最多返回的结果数量。若请求url中设置了size值http://router_server/$db_name/$space_name/_search?size=20优先使用url中指定的size值。
"""