# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   Project:
   File Name：
   Description :
   Author :        11103914
   date：          2020-03-03
-------------------------------------------------
   Change Activity:

-------------------------------------------------
"""

__author__ = '11103914'

import os
from datetime import datetime, timedelta
import requests
import json

def run(sql):
    print(sql)
    cmd = 'hive -e \"%s\"' % (sql)
    print(cmd)
    sql_result = os.popen(cmd).readlines()
    print("done")
    return sql_result

def get_date(day_shift=0):
    target_day = datetime.now() + timedelta(days=day_shift)

    target_day_format = target_day.strftime("%Y-%m-%d")

    print(target_day_format)
    return target_day_format

def get_data(day):
    '''
    :param data_path: 结果路径
    :param day: 查询数据day
    :return:
    '''

    sql = """
    select articleno,get_json_object(extraparam,'$.itemBertVec') as itemBertVec
    from ai_smart_launcher.r_feeds_data_analysis_result  
    where day between date_sub('{day}', 7) and '{day}'
    """.format(day=day)

    sql_result = run(sql)

    begin = datetime.now()
    f1 = open("/home/11103914/data/faiss_db/sl_data/data_sl.csv", "w", encoding="utf-8")
    f2 = open("/home/11103914/data/faiss_db/yw_data/data_yw.csv", "w", encoding="utf-8")
    f3 = open("/home/11103914/data/faiss_db/rec_data/data_rec.csv", "w", encoding="utf-8")
    f4 = open("/home/11103914/data/faiss_db/vivo_video_data/vivo_video.csv", "w", encoding="utf-8")

    cnt = 0
    for line in sql_result:
        line = line.split('\t')
        id = str(line[0])
        vec = str(line[1])
        data = id + "\t" + vec
        if id[0:3] in ['V01', 'V06', 'V07', 'V04', 'V12', 'V13', 'V14', 'V15'] or id[0] == "S":
            cnt += 1
        # 写入负一屏数据
        if id[0:3] in ['V01', 'V06', 'V07', 'V04', 'V12', 'V14'] and vec is not None:
            f1.write(data)
            # f1.write("\n")
        # 写入要闻数据
        if id[0:3] in ['V01', 'V04', 'V12'] and vec is not None:
            f2.write(data)
            # f2.write("\n")
        # 写入推荐数据
        if id[0:3] in ['V01', 'V04', 'V07', 'V12', 'V13', 'V15'] and vec is not None:
            f3.write(data)
            # f3.write("\n")
        # 写入vivo视频数据
        if id[0] == "S" and vec is not None:
            f4.write(data)
            # f4.write("\n")

        if cnt % 1000 == 0:
            print(cnt)
    f1.close()
    f2.close()
    f3.close()
    f4.close()
    print("write elapse: {}s".format((datetime.now() - begin).seconds))

def add_data(filePath, url, flag):
    f = open(filePath, "r", encoding="utf-8")
    adid = ""
    vector = []
    cnt = 0
    for line in f:
        line_str = line.strip().split("\t")
        adid = line_str[0]
        vector_str = line_str[1]
        vector_list_str = vector_str.split(",")
        try:
            for i in vector_list_str:
                vector.append(float(i))
        except Exception:
            cnt += 1
    print(cnt)
    f.close()
    #服务端口和IP
    url = url + "{}".format(adid)

    #请求体
    body = {'adid': adid, "vector": {"feature": vector}}

    #请求发送
    response = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))

    #结果检验，是否插入成功
    try:
        status = json.loads(response.text)["status"]
        if status == 201:
            print("{} data insert is ok".format(flag))
        else:
            # 此处应该抛出异常
            print("{} is not insert success".format(adid))
    except Exception:
        # 此处应该捕获异常，并作出处理
        print("insert {} data error!".format(flag))

if __name__ == "__main__":
    # day = get_date(day_shift=0)
    # get_data(day)

    sl_path = "/home/11103914/data/faiss_db/sl_data/data_sl.csv"
    yw_path = "/home/11103914/data/faiss_db/yw_data/data_yw.csv"
    rec_path = "/home/11103914/data/faiss_db/rec_data/data_rec.csv"
    vivo_video_path = "/home/11103914/data/faiss_db/vivo_video_data/vivo_video.csv"

    sl_url = "http://10.192.23.145:9001/content_platform_db/sl_faiss/"
    yw_url = "http://10.192.23.145:9001/content_platform_db/yw_faiss/"
    rec_url = "http://10.192.23.145:9001/content_platform_db/rec_faiss/"
    vivo_video_url = "http://10.192.23.145:9001/content_platform_db/vivo_video_faiss/"

    add_data(sl_path, sl_url, "sl")
    add_data(yw_path, yw_url, "yw")
    add_data(rec_path, rec_url, "rec")
    # add_data(vivo_video_path, vivo_video_url, "vivo_video")

