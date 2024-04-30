# Vector Function

## txt2vec(host,text)

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
dingo> select id,age, gender,hobby, color_index$distance from vector(VECTOR_TABLE_L2, color, txt2vec('ip:port','red'), 10) where age>1;
```

## img2vec(host,img_url,local_path)

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
dingo> select id,age, gender,hobby, color_index$distance from vector(VECTOR_TABLE_IMG_L2, color, img2vec('ip:port','img_url', false), 10) where age>1;
```

## distance(vector_column,txt2vec(host,text))

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
dingo> select age from VECTOR_TABLE_L2 where distance(color,txt2vec('ip:port','red')) < 0.1;
```
