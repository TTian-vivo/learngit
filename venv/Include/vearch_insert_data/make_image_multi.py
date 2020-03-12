


def post(imageid,index):
    #服务端口和IP
    url = "http://10.102.23.107:4101/test/imageSpace/{}".format(index)
    #请求体
    body = {'url':imageid, "vector": {"feature": imageid}}
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
    start=time.time()
    pool = multiprocessing.Pool(processes=8)
    ##现在是向服务中插入数据
    list_image=["/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000123599.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000164.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002764.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002923.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000073.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000192.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002822.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000007522.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000074.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000196.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002839.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000045535.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000136.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000139.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000143.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000000208.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002690.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002753.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002759.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002867.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002881.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002890.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000002894.jpg",
                "/root/go/src/github.com/vearch/vearch/plugin/images/image_retrieval/test/COCO_val2014_000000095375.jpg"]

    for imageid in list_image:
        index=list_image.index(imageid)
        pool.apply_async(post, (imageid,index))
    pool.close()
    pool.join()
    end=time.time()
    print(end-start)
    print("insert data is over！")