from annoy import AnnoyIndex
#from flask import Flask, request
# import time
# import json

def item_vectors():
    path_item_vector = "/home/app/article_wordvec"
    item_vector = []
    #item_id = []
    with open(path_item_vector, 'r') as f:
        item_info_string = f.read()
        each_item_info_list = item_info_string.split("\n")
        for each_item_info in each_item_info_list:
            if each_item_info != "":
                each_item_list = each_item_info.split("\t")
                each_vector_string = each_item_list[-1]
                each_vector_list = each_vector_string.split(",")
                try:
                    item_vector.append(list(map(float, each_vector_list)))
                    #item_id.append(each_item_list[0])
                except Exception:
                    pass
    return item_vector

# now = time.strftime("%Y-%m-%d", time.localtime())
# app = Flask(__name__)
print("start get data")
vector = item_vectors()

DIM = 300
i_annoy = []
for i in range(len(vector)):
    i_annoy.append(i)

print("start to add")
index = AnnoyIndex(DIM, 'angular')

for i in range(len(vector)):
    index.add_item(i_annoy[i],vector[i])

print("start to build")
index.build(2)

index.save("trees_2.ann")

# @app.route("/annoy_service/", methods=['GET'])
# def annoy_service():
#     try:
#         print("start analysis")
#         nprobe = int(request.args.get("nprobe"))
#         top_K = int(request.args.get("top_n"))
#         data_string = request.args.get("data")
#         data_string_split = data_string.split(",")
#         print("end analysis")
#
#
#         print("start add data")
#         v_search = []
#         for i in range(len(data_string_split)):
#             v_search.append(float(data_string_split[i]))
#         print("end add data")
#
#         print("start search vector")
#         result = index.get_nns_by_vector(v_search, top_K, search_k=nprobe, include_distances=False)
#         print(type(result))
#
#         result_data = {}
#         result_data["status"] = "success"
#         result_data["results"] = str(result)
#         result_json = json.dumps(result_data)
#         return result_json
#
#     except AssertionError as e1:
#         return e1
#     except ValueError as e2:
#         return e2

# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000, use_reloader=False)