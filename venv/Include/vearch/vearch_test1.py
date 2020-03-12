import vearch
import numpy as np

engine_path = "files"
max_doc_size = 1000
engine = vearch.Engine(engine_path, max_doc_size)
log_path = "logs"
engine.init_log_dir(log_path)


table = {
    "name" : "test_table",
    "model" : {
        "name": "IVFPQ",
        "nprobe": 2,
        "metric_type": "L2",
        "ncentroids": 2,
        "nsubvector": 64
    },
    "properties" : {
        "adid": {
            "type": "string"
        },
        "feature": {
            "type": "vector",
            "dimension": 128
        },
    },
}
engine.create_table(table)

profiles={}
add_num = 100
features = np.random.rand(add_num, 128)
doc_items = []
for i in range(add_num):
    profiles["adid"] = str(i)
    profiles["feature"] = features[i,:]
    doc_items.append(profiles)

#pass list to it, even only add one doc item
engine.add(doc_items)


query =  {
    "vector": [{
        "field": "feature",
        "feature": features[0,:],
    }],
}
result = engine.search(query)
print(result)