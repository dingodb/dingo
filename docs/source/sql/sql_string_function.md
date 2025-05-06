# String Function
## List
| Function Type | Description  |Return Value Type|
|-----------|--------|--------|
| Concat( )|Returns a spliced string.|String
Format( )|Returns a number formatted to the specified number of decimal places.|Decimal
Locate( )|Returns the position of the first occurrence of the substring.|Int
Lower( )|Returns the argument in lowercase.|String
Lcase( )|Returns the argument in lowercase.|String
Upper( )|Returns a parameter in uppercase.|String
Ucase( )|Returns the argument in uppercase.|String
Left( )|Returns the specified number of leftmost characters.|String
Right( )|Returns the specified number of rightmost characters.|String
Repeat( )|Repeats a string the specified number of times.|String
Replace( )|Replaces occurrences of the specified string.|String
Trim( )|Removes leading and trailing spaces.|String
Ltrim( )|Removes leading spaces.|String
Rtrim( )|Removes trailing spaces.|String
Mid( )|Returns a substring starting at the specified position.|String
SubString( )|Returns a substring starting at the specified position.|String
Reverse( )|Reverses characters in a string.|String
## Specification
### 1. Concat

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

### 2. Format

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

### 3. Locate

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

### 4. Lower

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

### 5. Lcase

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

### 6. Upper

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

### 7. Ucase

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

### 8. Left

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

### 9. Right(str,x)

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

### 10. Repeat

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

### 11. Replace

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

### 12. Trim

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


### 13. Ltrim

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

### 14. Rtrim

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

### 15. Mid

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

### 16. SubString

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

### 17. Reverse

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