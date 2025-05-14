# Play with DingoDB using DingoClient

In order to be more faster, DingoDB presents API which is comprehensive and powerful to do operations on the database, such as DDL or DML operation.

## Examples

-  Function about Dingo sdk: [SDK Documents](https://github.com/dingodb/pydingo/tree/master/examples)

### 1. Document_index

```
from dingodb import SDKDocumentDingoDB, SDKClient
from dingodb.common.document_rep import DocumentType, DocumentColumn, DocumentSchema


addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)

x = SDKDocumentDingoDB(sdk_client)
print(x)

index_name = "document_index_test"
```
#### delete_index
```
delete_index_out = x.delete_index(index_name)
print(delete_index_out)
import time
```
#### create_index
```
scheme =  DocumentSchema()
col = DocumentColumn("text", DocumentType.STRING)
scheme.add_document_column(col)
col = DocumentColumn("i64", DocumentType.INT64)
scheme.add_document_column(col)
col = DocumentColumn("f64", DocumentType.DOUBLE)
scheme.add_document_column(col)
col = DocumentColumn("bytes", DocumentType.BYTES)
scheme.add_document_column(col)
col = DocumentColumn("bool", DocumentType.BOOL)
scheme.add_document_column(col)
col = DocumentColumn("datetime", DocumentType.DATETIME)
scheme.add_document_column(col)

create_index_out = x.create_index(index_name, scheme, 3, operand=[5, 10, 20])
# create_index_out = x.create_index(index_name, scheme, 3)
print(create_index_out)
time.sleep(5)
```
#### make dataset
```
ids = [3, 5, 7, 9, 11, 13, 15, 17, 19, 21]
documents = [
    {"text" : "Ancient empires rise and fall, shaping history's course.", "i64" : 1003, "f64" : 1003.0, "bytes" : "bytes_data_3","bool":True, "datetime": "2021-01-01T00:00:00Z"},
    {"text" : "Artistic expressions reflect diverse cultural heritages.", "i64" : 1005, "f64" : 1005.0, "bytes" : "bytes_data_5","bool":False, "datetime": "2021-01-01T00:00:00Z"},
    {"text" : "Social movements transform societies, forging new paths.", "i64" : 1007, "f64" : 1007.0, "bytes" : "bytes_data_7","bool":True, "datetime": "2022-01-01T00:00:00Z"},
    {"text" : "Economies fluctuate, reflecting the complex interplay of global forces.", "i64" : 1009, "f64" : 1009.0, "bytes" : "bytes_data_9","bool":False, "datetime": "2022-01-01T00:00:00Z"},
    {"text" : "Strategic military campaigns alter the balance of power.", "i64" : 1011, "f64" : 1011.0, "bytes" : "bytes_data_11","bool":True, "datetime": "2023-01-01T00:00:00Z"},
    {"text" : "Quantum leaps redefine understanding of physical laws.", "i64" : 1013, "f64" : 1013.0, "bytes" : "bytes_data_13","bool":False, "datetime": "2023-01-01T00:00:00Z"},
    {"text" : "Chemical reactions unlock mysteries of nature.", "i64" : 1015, "f64" : 1015.0, "bytes" : "bytes_data_15","bool":True, "datetime": "2024-01-01T00:00:00Z"},
    {"text" : "Philosophical debates ponder the essence of existence.", "i64" : 1017, "f64" : 1017.0, "bytes" : "bytes_data_17","bool":False, "datetime": "2024-01-01T00:00:00Z"},
    {"text" : "Marriages blend traditions, celebrating love's union.", "i64" : 1019, "f64" : 1019.0, "bytes" : "bytes_data_19","bool":True, "datetime": "2025-01-01T00:00:00Z"},
    {"text" : "Explorers discover uncharted territories, expanding world maps.", "i64" : 1021, "f64" : 10021.0, "bytes" : "bytes_data_21","bool":False, "datetime": "2025-01-01T00:00:00Z"}
]

document_add_out = x.document_add(index_name, documents, ids)
print(document_add_out)
print(document_add_out.to_dict())
```
#### document_search
```
document_search_out = x.document_search(index_name, "discover", 5, with_scalar_data=True)
print(document_search_out)
print(document_search_out.to_dict())
```
```
document_search_out = x.document_search(index_name, "of", 3, with_scalar_data=True)
print(document_search_out)
```
```
document_search_out = x.document_search(index_name, "of", 5,[13, 15], with_scalar_data=True)
print(document_search_out)
```
```
document_search_out = x.document_search_all(index_name, "of", with_scalar_data=True, query_limit=4096)
print(document_search_out)
```
```
document_search_out = x.document_search(index_name, r"(text:'of' AND i64: >= 1013)", 5,  [9, 11, 13, 15], with_scalar_data=True)
print(document_search_out)
```
```
document_search_out = x.document_search(index_name, r"( bool:true)", 5,  [9, 11, 13, 15], with_scalar_data=True)
print(document_search_out)
```
```
document_search_out = x.document_search(index_name, r"(datetime:'2023-01-01T00:00:00Z' )", 5,  [3,5,7,9, 11, 13, 15], with_scalar_data=True)
print(document_search_out)
```
#### document_query
```
document_query_out = x.document_query(index_name, ids, True, ["text", "i64"])
print(document_query_out)
```
#### document_get_border
```
document_get_border_out = x.document_get_border(index_name, True)
print(document_get_border_out)
document_get_border_out = x.document_get_border(index_name, False)
print(document_get_border_out)
```
#### document_scan_query
```
document_scan_query_out = x.document_scan_query(index_name, ids[0], ids[-1], False, 2)
print(document_scan_query_out)
document_scan_query_out = x.document_scan_query(index_name, ids[-1], ids[0], True, 2)
print(document_scan_query_out)
```
```
document_scan_query_out = x.document_scan_query(index_name, ids[0], ids[-1] + 10, False, 100, True, ["text", "i64"])
print(document_scan_query_out)
```
#### document_index_metrics
```
document_metrics_out = x.document_index_metrics(index_name)
print(document_metrics_out)
```
#### document_count_out
```
document_count_out = x.document_count(index_name, 0, 19)
print(document_count_out)
```
#### document_delete
```
document_delete_out = x.document_delete(index_name, ids)
print(document_delete_out)
```

### 2. Document_regex_index
#### import
```
from dingodb import SDKDocumentDingoDB, SDKClient
from dingodb.common.document_rep import DocumentType, DocumentColumn, DocumentSchema


addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)

x = SDKDocumentDingoDB(sdk_client)
print(x)

index_name = "document_regex_index_test"
```
#### delete_index
```
delete_index_out = x.delete_index(index_name)
print(delete_index_out)
```
#### create_index
```
import time

scheme =  DocumentSchema()
col = DocumentColumn("title", DocumentType.STRING)
scheme.add_document_column(col)
col = DocumentColumn("text", DocumentType.STRING)
scheme.add_document_column(col)

create_index_out = x.create_index(index_name, scheme, 3, operand=[5, 10, 20])
# create_index_out = x.create_index(index_name, scheme, 3)
print(create_index_out)
time.sleep(5)
```
#### make dataset
```
ids = [1, 2, 3]
documents = [
    {"title" : "a", "text" : "The Diary of Muadib"},
    {"title" : "bb", "text" : "A Dairy Cow"},
    {"title" : "ccc", "text" : "The Diary of a Young Girl"}
]

document_add_out = x.document_add(index_name, documents, ids)
print(document_add_out)
```
#### DocumentRegexSearch
```
#  base64encode Dia.* to RGlhLioq
#  text contains "Dia"
document_search_out = x.document_search(index_name, "text:RE [RGlhLio=]", 5, False, with_scalar_data=True)
print(document_search_out)
```
#### DocumentSearchLength
```
#  base64encode (.{0,2})  to KC57MCwyfSk=
#  title length <= 2
document_search_out = x.document_search(index_name, "title:RE [KC57MCwyfSk=]", 5, False, with_scalar_data=True)
print(document_search_out)
```
#### DocumentSearchAnd
```
#  base64encode (.{0,2})  to KC57MCwyfSk=
#  base64encode Dia.* to RGlhLioq
#  title length <= 2 and text contains "Dia"
document_search_out = x.document_search(index_name, "title:RE [KC57MCwyfSk=] AND text:RE [RGlhLio=]", 5, False, with_scalar_data=True)
print(document_search_out)
```

### 3. Rawkv
#### import
```
import numpy as np
import os

from dingodb import SDKRawKVDingoDB, SDKClient

# need to create region ( range(wa,wc) for this example ) before using SDKRawKVDingoDB

addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)
x = SDKRawKVDingoDB(sdk_client)
print(x)
```
#### put
```
x.rawkv_put("wb01", "value1")
```
#### batch_put
```
x.rawkv_batch_put([
    ("wb02", "value2"),
    ("wb03", "value3"),
    ("wb04", "value4"),
    ("wb05", "value5"),
    ("wb06", "value6"),
    ("wb07", "value7"),
    ("wb08", "value8"),
    ("wb09", "value9"),
    ("wb10", "value10"),
    ("wb11", "value11"),
    ("wb12", "value12"),
    ("wb13", "value13"),
    ("wb14", "value14"),
    ("wb15", "value15")])
```
#### get
```
x.rawkv_get("wb01")
```
#### kv.to dict
```
[kv.to_dict() for kv in x.rawkv_batch_get([
    "wb01",
    "wb02",
    "wb03",
    "wb04",
    "wb05",
    "wb06",
    "wb07",
    "wb08",
    "wb09",
    "wb10",
    "wb11",
    "wb12",
    "wb13",
    "wb14",
    "wb15"
])]
```
```
x.rawkv_put_if_absent("wb16", "value16")
```
#### delete
```
x.rawkv_delete("wb16")
```
```
x.rawkv_batch_delete([
    "wb01",
    "wb02",
])
```
```
[kv.to_dict() for kv in x.rawkv_batch_put_if_absent([
    ("wb01", "value1"),
    ("wb02", "value2"),
    ("wb03", "value3"),
    ("wb04", "value4"),
    ("wb05", "value5"),
    ("wb06", "value6"),
    ("wb07", "value7"),
    ("wb08", "value8"),
    ("wb09", "value9"),
    ("wb10", "value10"),
    ("wb11", "value11"),
    ("wb12", "value12"),
    ("wb13", "value13"),
    ("wb14", "value14"),
    ("wb15", "value15")])]
```
```
[kv.to_dict() for kv in x.rawkv_scan("wb", "wc", 15)]
```
#### delete range
```
x.rawkv_delete_range("wa", "wc")
```
### 4. Region-creator
#### import
```
import numpy as np
import os

from dingodb import SDKRegionCreatorDingoDB, SDKClient

addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)
x = SDKRegionCreatorDingoDB(sdk_client)
print(x)
```
#### drop_region
```
x.drop_region(80031)
```
#### create_region
```
x.create_region_id(10)
```
```
x.create_region("test1","wb00000000","wc000000",80033)
```

### 5. Vector_index
```
import numpy as np
import os

from dingodb import SDKVectorDingoDB, SDKClient
from dingodb.common.vector_rep import ScalarType, ScalarColumn, ScalarSchema

addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)
x = SDKVectorDingoDB(sdk_client)
print(x)

index_name = "test_index_grpc1"
```
#### delete_index
```
x.delete_index(index_name)
```
#### create_index
```
# help(x.create_index)
#x.create_index(index_name, 8, "binary_flat", "hamming", 3, operand=[5,10,15,20])
col = ScalarColumn("id",ScalarType.DOUBLE,True)
sca = ScalarSchema()
sca.add_scalar_column(col)
x.create_index_with_schema(index_name, 16,sca, "binary_ivf_flat", "hamming", 3, operand=[100,500,1500,3000,6000])
```
#### make dataset
```
d = 16                           # dimension
bd = 2                     #binary dimension
nb = 4                      # database size
np.random.seed(1234)             # make reproducible
xb = np.random.randint(0, 255, (nb, bd))  # 生成范围为 0-255 的随机整数
print(xb.shape)
xb[:, 0] += np.arange(nb) 
print(xb)
print(xb.shape)

ids = [1, 2, 3, 4]
datas = [{"id": 50}, {"id": 120}, {"id": 130}, {"id": 4.40}]
vectors = xb.tolist()
```
#### add
```
for i in range(10):
    x.vector_add(index_name, datas, vectors, ids,"binary")
```
#### delete
```
x.vector_delete(index_name,ids)
```
#### get_auto_increment_id
```
x.vector_get_auto_increment_id(index_name)
```
#### update_auto_increment_id
```
x.vector_update_auto_increment_id(index_name,16)
```
#### upsert
```
x.vector_upsert(index_name, datas, vectors, ids,"binary")
```
#### search
```
# vector_search
x.vector_search(index_name, vectors[0],value_type="binary")
# return 
    # error RuntimeError
```
#### search with pre_filter or post_filter
```
x.vector_search(index_name, vectors[0], 10, {"meta_expr": {"id": 1}},value_type="binary")
```
#### get index with id
```
x.vector_get(index_name, [1, 2, 6])
```
#### Add: scan
```
x.vector_scan(index_name, 20, 60,is_reverse=True,end_id=0)
```
#### Add count 
```
x.vector_count(index_name)
```
#### metrics
```
x.vector_metrics(index_name)
```
#### get_max_index_row
```
x.get_max_index_row(index_name)
```
#### delete_index
```
# delete_index
x.delete_index(index_name)
```
### 6. Vector_binary_index
#### import
```
import numpy as np
import os

from dingodb import SDKVectorDingoDB, SDKClient
from dingodb.common.vector_rep import ScalarType, ScalarColumn, ScalarSchema

addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003"
sdk_client = SDKClient(addrs)
x = SDKVectorDingoDB(sdk_client)
print(x)

index_name = "test_index_grpc1"
```
#### delete_index
```
x.delete_index(index_name)
```
#### create_index
```
# help(x.create_index)
#x.create_index(index_name, 8, "binary_flat", "hamming", 3, operand=[5,10,15,20])
col = ScalarColumn("id",ScalarType.DOUBLE,True)
sca = ScalarSchema()
sca.add_scalar_column(col)
x.create_index_with_schema(index_name, 16,sca, "binary_ivf_flat", "hamming", 3, operand=[100,500,1500,3000,6000])
```
#### make dataset
```
d = 16                           # dimension
bd = 2                     #binary dimension
nb = 4                      # database size
np.random.seed(1234)             # make reproducible
xb = np.random.randint(0, 255, (nb, bd))  # 生成范围为 0-255 的随机整数
print(xb.shape)
xb[:, 0] += np.arange(nb) 
print(xb)
print(xb.shape)

ids = [1, 2, 3, 4]
datas = [{"id": 50}, {"id": 120}, {"id": 130}, {"id": 4.40}]
vectors = xb.tolist()
```
#### vector_add
```
for i in range(10):
    x.vector_add(index_name, datas, vectors, ids,"binary")
```
#### vector_delete
```
x.vector_delete(index_name,ids)
```
#### vector_get_auto_increment
```
x.vector_get_auto_increment_id(index_name)
```
#### vector_update_auto_increment
```
x.vector_update_auto_increment_id(index_name,16)
```
#### vector_upsert
```
x.vector_upsert(index_name, datas, vectors, ids,"binary")
```
#### vector_search
```
x.vector_search(index_name, vectors[0],value_type="binary")
# return 
    # error RuntimeError
```
```
# vector_search with pre_filter or post_filter
x.vector_search(index_name, vectors[0], 10, {"meta_expr": {"id": 1}},value_type="binary")
```
#### vector_getS
```
x.vector_get(index_name, [1, 2, 6])
```
#### vector_scan
```
x.vector_scan(index_name, 20, 60,is_reverse=True,end_id=0)
```
#### Add vector count 
```
x.vector_count(index_name)
```
#### vector_metrics
```
x.vector_metrics(index_name)
```
#### get_max_index_row
```
x.get_max_index_row(index_name)
```
#### delete_index
```
x.delete_index(index_name)
```
### 7.Vector_diskann_index
#### import
```
import numpy as np
import os

from dingodb import SDKVectorDingoDB, SDKClient
from dingodb.common.vector_rep import ScalarType, ScalarColumn, ScalarSchema

addrs = "172.30.14.11:22001,172.30.14.11:22002,172.30.14.11:22003"
sdk_client = SDKClient(addrs)
x = SDKVectorDingoDB(sdk_client)
print(x)

index_name = "test_index_grpc"
```
#### delete_index
```
x.delete_index(index_name)
```
#### create_index
```
# help(x.create_index)
x.create_index(index_name, 6, "diskann", "euclidean", 3, index_config={"valueType": "float","searchListSize": 100,"maxDegree": 64}, operand=[5,10,15,20])
```
#### make dataset
```
d = 6                           # dimension
nb = 4                      # database size
np.random.seed(1234)             # make reproducible
xb = np.random.random((nb, d)).astype('float32')
print(xb)
print(xb.shape)
xb[:, 0] += np.arange(nb) / 1000.
print(xb)
print(xb.shape)

ids = [1, 2, 3, 4]
datas = [{"a1": "b1"}, {"a2": "b2"}, {"a3": "b3"}, {"a4": "b4"}]
vectors = xb.tolist()
```
#### vector_add
```
x.vector_import_add(index_name, datas, vectors, ids)
x.vector_import_add(index_name, datas, vectors, ids)
x.vector_import_add(index_name, datas, vectors, ids)
x.vector_import_add(index_name, datas, vectors, ids)
```
#### vector_build_by_index
```
x.vector_build_by_index(index_name)
```
#### r.to_dict
```
[r.to_dict() for r in x.vector_status_by_index(index_name)]
```
#### vector_count_memory
```
x.vector_count_memory(index_name)
```
#### vector_search
```
x.vector_search(index_name, vectors[0])
```
#### r.to_dict
```
[r.to_dict() for r in x.vector_load_by_index(index_name)]
```
```
[r.to_dict() for r in x.vector_reset_by_index(index_name)]
```

```
id = [1,80001]
[r.to_dict() for r in x.vector_status_by_region(index_name,id)]
```

```
[r.to_dict() for r in x.vector_build_by_region(index_name,id)]
```

```
[r.to_dict() for r in x.vector_load_by_region(index_name,id)]
```

```
[r.to_dict() for r in x.vector_reset_by_region(index_name,id)]
```
#### delete
```
x.vector_import_delete(index_name,ids)
```

```
x.delete_index(index_name)
```