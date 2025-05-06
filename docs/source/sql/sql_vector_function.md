# Vector Function
## List
| Function Type | Description  |Return Value Type|
|-----------|--------|--------|
| txt2vec( host,test)|Extract vectors from text, measure vectors as 1024 dimensions|Float
|Img2vec(host,img_url,local_path)|Extracts vectors from images, the measurement vector is 512 dimensional|Float
|distance(vector_column, txt2vec( host,test))|Calculate the distance between two vectors, the distance type is the distance type of the specified vector column in the vector table.|Float
|ipDistance(vector_column, array[1,2,3…])|The inner product distance function, a method to calculate the similarity between vectors. A larger distance means the vectors are less similar, a smaller distance means the vectors are more similar.|Float
|l2distance(vector_column, array[1,2,3…])|Euclidean distance function, used to measure the distance between two points in a straight line, it is more commonly used for vectors with continuous values. The shorter the distance, the more similar.|Float
|cosineDistance(vector_column, array[1,2,3…])|The cosine distance function, a common measure of the similarity of vectors, the closer the value to 1, the more similar the vectors are, and the closer the value to 0, the less similar the vectors are.|Float
## Specification
### 1. txt2vec(host,text)

Extract vectors from the text, with a test vector of 1024 dimensions.

- Syntax

```
txt2vec('host','text')
```

- Parameter

| Parameter | Type   | Description                                                                  | Required |
|-----------|--------|------------------------------------------------------------------------------|----------|
| host      | String | The specific IP: port of the model service needs to include ''               | Y        |
| text      | String | Indicates the text that needs to extract vectors, which needs to contain ''  | Y        |

- Return Type

```
Float
```

- Examples

```sql
0: jdbc:dingo::///> select id,age, gender,hobby, color_index$distance from vector(VECTOR_TABLE_L2, color, txt2vec('ip:port','red'), 10) where age>1; 
```

### 2. img2vec(host,img_url,local_path)

Extract vectors from the image, with a test vector of 512 dimensions.

- Syntax

```
img2vec('host','img_url','local_path')
```

- Parameter

| Parameter  | Type    | Description                                                                              | Required |
|------------|---------|------------------------------------------------------------------------------------------|----------|
| host       | String  | The specific IP: port of the model service needs to include ''                           | Y        |
| img_url    | String  | The network address or local address (server) representing the image needs to include '' | Y        |
 | local_path | Boolean | Indicates whether to obtain local image resources, including '';Default value: false     | N        |
- Return Type

```
Float
```

- Examples

```sql
0: jdbc:dingo::///> select id,age, gender,hobby, color_index$distance from vector(VECTOR_TABLE_IMG_L2, color, img2vec('ip:port','img_url', false), 10) where age>1; 
```

### 3. distance(vector_column,txt2vec(host,text))

Calculate the distance between two vectors, using the distance metric specified for the vector column in the vector table. The two vectors must have the same dimensions.

- Syntax

```
distance('vector_column',txt2vec(host,text))
```

- Parameter

| Parameter          | Type   | Description                                                                                  | Required |
|--------------------|--------|----------------------------------------------------------------------------------------------|----------|
| vector_column      | String | The column name of the vector column used for distance calculation, without quotation marks. | Y        |
| txt2vec(host,text) |        | The vector function used for distance calculation.                                           | Y        |

- Return Type

```
Float
```

- Examples

```sql
0: jdbc:dingo::///> select age from VECTOR_TABLE_L2 where distance(color,txt2vec('ip:port','red')) < 0.1; 
```

### 4. ipDistance(vector_colume, array[1,2,3…])

The inner product distance calculation function, a method for calculating the similarity between vectors. A larger distance means the vectors are less similar, and a smaller distance means the vectors are more similar.

- Parameter

| Parameter          | Type   | Description                                                                                  | Required |
|--------------------|--------|----------------------------------------------------------------------------------------------|----------|
| vector_column      | String | Indicates the name of the column for which the distance is to be requested,without quotation marks. | Y        |
| array[1,2…] |  Float      | Indicates the distance value to be calculated        | Y        |

- Syntax 1

```
<*>
```

- Examples 1
```sql
select  feature <*> array[1.1,1.2,1.3] from table;
```
- Syntax 2

```
ipdistance(vector_colume, array[])
```
- Examples 2

```sql
select ipDistance(feature,array[1,2......]) from table;
```

### 5. l2Distance(vector_colume, array[1,2,3…])

Measures the L2 distance between 2 points. That is, the length of the line between the points represented by the vectors; the shorter the distance, the more similar the source objects are conceptually.

- Parameter

| Parameter          | Type   | Description                                                                                  | Required |
|--------------------|--------|----------------------------------------------------------------------------------------------|----------|
| vector_column      | String | Indicates the name of the column for which the distance is to be requested,without quotation marks. | Y        |
| array[1,2…] |   Float     | Indicates the distance value to be calculated              | Y        |

- Syntax 1

```
<=>
```
- Examples 1

```sql
select  feature <=> array[1.1,1.2,1.3] from table;
```
- Syntax 2

```
l2distance(vector_colume, array[])
```
- Examples 2

```sql
select l2Distance(feature,array[1,2......]) from table;
```

### 6. cosDistance(vector_colume, array[1,2,3…])

Measures the cosine of the angle between two vectors, i.e., the dot product divided by the length.

- Parameter

| Parameter          | Type   | Description                                                                                  | Required |
|--------------------|--------|----------------------------------------------------------------------------------------------|----------|
| vector_column      | String | The column name of the vector column used for distance calculation, without quotation marks. | Y        |
| txt2vec(host,text) |        | The vector function used for distance calculation.                                           | Y        |

- Syntax 1

```
<->
```
- Examples 1

```sql
select  feature <-> array[1.1,1.2,1.3] from table;
```
- Syntax 2

```
cosdistance(vector_colume,array[])
```

- Examples 2

```sql
select cosDistance(feature,array[1,2......]) from table;
```
