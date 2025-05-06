# Aggregate Function
## List
| Function Type | Description  |Return Value Type|
|-----------|--------|--------|
| AVG|Returns the average value|Double
| COUNT|Returns the number of records|Int
|SUM|Calculates the sum of values|Double, Int
|MAX|Returns the maximum value|Float
|MIN|Returns the minimum value|Float

## Specification

### 1. AVG

The AVG() function returns the average value of an expression.

- Syntax

```
AVG()
```

- Return Type

```
Double
```

- Examples

```sql
dingo> select AVG(amount) from Student;
+--------+
| EXPR$0 |
+--------+
| 98.5   |
+--------+
```

### 2. COUNT

 The COUNT() function returns the number of records returned by a SELECT query.

- Syntax

```
Count()
```

- Return Type

```
Int
```

- Examples

```
dingo>select Count(id) from Student;
+--------+
| EXPR$0 |
+--------+
| 5      |
+--------+
```

### 3. SUM

The SUM() function calculates the sum of values.

- Syntax

```
Sum()
```

- Return Type

```
A double if the input type is double, otherwise integer.
```

- Examples

```sql
dingo>select Count(AGE) from Student;
+--------+
| EXPR$0 |
+--------+
| 5      |
+--------+

dingo>select Sum(Amount) from Student;
+--------+
| EXPR$0 |
+--------+
| 492.5  |
+--------+
```

### 4. MAX

The MAX() function returns the maximum value.

- Syntax

```
Max()
```

- Return Type
   
```
Object
```

- Examples

```sql
dingo>select Max(AGE) from Student;
+--------+
| EXPR$0 |
+--------+
| 22     |
+--------+
```

### 5. MIN

The MIN() function returns the minimum value.

- Syntax

```
Min()
```

- Return Type

```
Object
```

- Examples
   
```sql
dingo>select Min(AGE) from Student;
+--------+
| EXPR$0 |
+--------+
| 13     |
+--------+
```
