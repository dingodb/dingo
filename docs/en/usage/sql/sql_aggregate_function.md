# Aggregate Function

## Types of aggregate

### AVG

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

### COUNT

 The COUNT() function returns the number of records returned by a SELECT query.

- Syntax

```
Count()
```

- Return Type

```
Integer
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

### SUM

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

### MAX

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

### MIN

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
