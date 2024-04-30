# String Function

## Concat

Return concatenated string.

- Syntax

```
str1 || str2 ||.....|| strn
```

- Return Type

```
String
```

- Examples

```sql
dingo> Select 'a'||'b'||'c';
+--------+
| EXPR$0 |
+--------+
| abc    |
+--------+
```

## Format

Return a number formatted to specified number of decimal places.

- Syntax

```
Format(x,n)
```

- Return Type

```
String
```

- Examples

```sql
dingo>select Format(123123.456,2);
+-----------+
|  EXPR$0   |
+-----------+
| 123123.46 |
+-----------+
```

## Locate

Return the position of the first occurrence of substring.

- Syntax

```
Locate(substr,str)
```

- Return Type

```
Integer
```

- Examples

```
dingo> Select Locate('a','abc');
+--------+
| EXPR$0 |
+--------+
| 1      |
+--------+
```

## Lower

Return the argument in lowercase.

- Syntax

```
Lower(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Lower('ABC');
+--------+
| EXPR$0 |
+--------+
| abc    |
+--------+
```

## Lcase

Return the argument in lowercase.

- Syntax

```
Lcase(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Lcase('ABC');
+--------+
| EXPR$0 |
+--------+
| abc    |
+--------+
```

## Upper

Return the argument in uppercase.

- Syntax

```
Upper(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Upper('abc');
+--------+
| EXPR$0 |
+--------+
| ABC    |
+--------+
```

## Ucase

Return the argument in uppercase.

- Syntax

```
Ucase(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Ucase('Abc');
+--------+
| EXPR$0 |
+--------+
| ABC    |
+--------+
```

## Left

Return the leftmost number of characters as specified.

- Syntax

```
Left(str,x)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Left('Alice',1);
+--------+
| EXPR$0 |
+--------+
| A      |
+--------+
```

## Right(str,x)

Return the specified rightmost number of characters.

- Syntax

```
Right(str,x)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Right('Alice',2);
+--------+
| EXPR$0 |
+--------+
| ce     |
+--------+
```

## Repeat

Repeat a string the specified number of times.

- Syntax

```
Repeat(str,n)
```

- Return Type

```
String
```

- Examples

```
dingo>Select Repeat('Abc',3);
+-----------+
|  EXPR$0   |
+-----------+
| AbcAbcAbc |
+-----------+
```

## Replace

Replace occurrences of a specified string.

- Syntax

```
Replace(str,a,b)
```

- Return Type

String


- Examples

```sql
dingo>Select Replace('ABCAbcaBc','A','a');
+-----------+
|  EXPR$0   |
+-----------+
| aBCabcaBc |
+-----------+
```

## Trim

Remove leading and trailing spaces.

- Syntax

```
Trim(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Trim('   A   B   C    ');
+-----------+
|  EXPR$0   |
+-----------+
| A   B   C |
+-----------+
```


## Ltrim

Remove leading spaces.

- Syntax

```
Rtrim(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Ltrim('   A   B   C    ');
+---------------+
|    EXPR$0     |
+---------------+
| A   B   C     |
+---------------+
```

## Rtrim

Remove trailing spaces.

- Syntax

```
Rtrim(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Rtrim('   A   B   C    ');
+--------------+
|    EXPR$0    |
+--------------+
|    A   B   C |
+--------------+
```

## Mid

Return a substring starting from the specified position.

- Syntax

```
Mid(str,n,len)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select MID('ABCDEFG',3,4);
+--------+
| EXPR$0 |
+--------+
| CDEF   |
+--------+
```

## SubString

Return a substring starting from the specified position.

- Syntax

```
SubString(str,x,len)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select SubString('ABCDEFG',2,1);
+--------+
| EXPR$0 |
+--------+
| B      |
+--------+
```

## Reverse

Reverse the characters in a string.

- Syntax

```
Reverse(str)
```

- Return Type

```
String
```

- Examples

```sql
dingo>Select Reverse('ABC');
+--------+
| EXPR$0 |
+--------+
| CBA    |
+--------+
```