


DIM = 300


def item_vector():
    url_tmp = 'http://10.193.7.158:50070;http://10.193.7.28:50070'
    #hdfs://bj04-region04/region04/27141/app/develop/tag-faiss
    client = hdfs.InsecureClient(url=url_tmp, user='hdfs')
    path_list = []

    path="hdfs://bj04-region04/region04/27141/app/develop/tag-faiss/tag_data.2019110916.1"
    for i in range(len(path_list_dir)):
        path_list.append(
            "/region01/29962/app/develop/faiss_distribute_test/" + path_dif_scale_data + "/" + path_list_dir[i])
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
                    # index_temp = line_split[2]
                    data_list.append(data_str_tmp)
                    # index_list.append(index_temp)
            del data_hdfs
            del data_str
            del data_split
            gc.collect()
    xb = np.array(data_list).astype('float32')
    return xb

def create_index(xb):

    start_create_index_time = time.time()
    global index
    index_ = faiss.IndexFlatL2(DIM)
    index = faiss.IndexIVFFlat(index_, DIM, nlist)
    index.train(xb)
    end_create_index_time = time.time()
    print("the index create success,and the time is :::", end_create_index_time - start_create_index_time)

    start_add_data_time = time.time()
    index.add(xb)
    end_add_data_time = time.time()
    print("add data success,and the time is :::", end_add_data_time - start_add_data_time)