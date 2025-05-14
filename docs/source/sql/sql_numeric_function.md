# Numberical Function
## List
| Function Type | Description  |Return Value Type|
|-----------|--------|--------|
| Pow(X,Y)| Finds the Yth power of the parameter X| Decimal
| Round(X,Y)| Used for rounding data| Int、Double、Float
| Ceiling(X)| Returns the largest integer value greater than parameter X| Int
| Floor(X)| Returns the largest integer value less than the parameter X| Int
| Mod(X,Y)| Returns the remainder of X divided by Y| Decimal
| Abs()| Returns the absolute value of X| Decimal
## Specification
### 1. Pow(X,Y)

- Definition and Usage

Returns the Yth power of the argument X. When X or Y is null, null is returned.

> Note: This function is equal to the POW() function.


- Syntax

```sql
POWER(X, Y)
```

- Parameter Values

| Parameter |Type| Description                       |
|-----------|----|-------------------------------|
| X         |Int, Double, Float, Long| Required. A number (the base)     |
| Y         | Int|Required. A number (the exponent) |

- Example

```sql
0: jdbc:dingo::///> select Pow(123.123,3);
+-------------------+
|      EXPR$0       |
+-------------------+
| 1866455.185461867 |
+-------------------+
1 row selected (0.048 seconds)
```

### 2. Round(X,Y)

- Definition and Usage
Used for data rounding; Y parameter can be ignored, the default is 0;
When the parameter Y is a negative integer is, X in the decimal point to the left of the Y rounding (from right to left);
If the parameter Y is negative and greater than the number of digits before the decimal point, Round returns 0.

- Syntax

```sql
Round(X,Y)
```

- Parameter Values

| Parameter | Type|Description                                                                                                 |
|-----------|--------------------|-----------------------------------------------------------------------------------------|
| X	| Int, Double, Float  | Required. The number to be rounded                                                                          |
| Y  |Int| Optional. The number of decimal places to round number to. If omitted, it returns the integer (no decimals) |

- Examples

```sql
0: jdbc:dingo::///> select Round(123.4567,3);
+---------+
| EXPR$0  |
+---------+
| 123.457 |
+---------+
1 row selected (0.061 seconds)
```

### 3. Ceiling(X)


- Definition and Usage

Returns the largest integer value greater than the argument X, or null if X is null;
The abbreviation Ceil(X) may also be used.

> Note: This function is equal to the CEIL() function.

- Syntax

```sql
Ceiling(X)
```

- Parameter Values


| Parameter |Type| Description               |
|-----------|-----|----------------------|
| X    | Double, Float|Required. A numeric value |


- Examples

```sql
0: jdbc:dingo::///> select Ceiling(15.98);
+--------+
| EXPR$0 |
+--------+
| 16     |
+--------+
1 row selected (0.049 seconds)
```

### 4. Floor(X)

- Definition and Usage

Returns the largest integer value less than the argument X, or null if X is null.

- Syntax

```sql
Floor(X)
```

- Parameter Values

| Parameter | Type|Description               |
|-----------|------|---------------------|
| X    | Double, Float|Required. A numeric value |


- Examples

```sql
0: jdbc:dingo::///> select Floor(15.98);
+--------+
| EXPR$0 |
+--------+
| 15     |
+--------+
1 row selected (0.066 seconds)
```

### 5. Mod(X,Y)

- Definition and Usage

Returns the remainder of X divided by Y. Returns null if any of the arguments X and Y is null.

- Syntax

```sql
MOD(X, Y)
```

- Parameter Values


| Parameter | Type|Description                                 |
|-----------|-------|--------------------------------------|
| X         | Int, Double, Float, Long|Required. A value that will be divided by y |
| Y         | Int, Double, Float, Long|Required. The divisor                       |

- Examples

```sql
0: jdbc:dingo::///> select mod(39,5);
+--------+
| EXPR$0 |
+--------+
| 4      |
+--------+
1 row selected (0.083 seconds)
```

### 6. Abs(X,Y)

- Definition and Usage

Returns the absolute value of parameter X; if parameter X is null, returns null.

- Syntax

```sql
Abs(X)
```

- Parameter Values


| Parameter | Type|Description               |
|-----------|------|---------------------|
| X    | Int, BigInt, Double, Float|Required. A numeric value |

- Examples

```sql
0: jdbc:dingo::///> select Abs(-123.45);
+--------+
| EXPR$0 |
+--------+
| 123.45 |
+--------+
1 row selected (0.062 seconds)
```