import numpy as np
import time
import os
import json
import requests

def get_path_index():
    # 发送http请求，获取路径值
    path_json = requests.get("http://v-deploy-faiss-dis-test:5000/register/")
    path = json.loads(path_json.text)['path']
    print(path)
    print("*****************************************")
    return path



if __name__ == '__main__':

    path = get_path_index()
    os.system("python slave_flask.py %s"%(path))
