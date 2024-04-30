# Numberical Function

## Pow

- Definition and Usage

The POWER() function returns the value of a number raised to the power of another number.

> Note: This function is equal to the POW() function.


- Syntax

```sql
POWER(x, y)
```

- Parameter Values

| Parameter | Description                       |
|-----------|-----------------------------------|
| x         | Required. A number (the base)     |
| y         | Required. A number (the exponent) |


- Example

```sql
SELECT POWER(8, 3);
```

## Round

- Definition and Usage
The ROUND() function rounds a number to a specified number of decimal places.

> Note: See also the FLOOR(), CEIL(), CEILING() functions.

- Syntax

```sql
ROUND(number, decimals)
```

- Parameter Values

| Parameter | Description                                                                                                 |
|-----------|-------------------------------------------------------------------------------------------------------------|
| number	   | Required. The number to be rounded                                                                          |
| decimals  | Optional. The number of decimal places to round number to. If omitted, it returns the integer (no decimals) |

- Examples

```sql
SELECT ROUND(345.156, 0);
```

## Ceiling


- Definition and Usage

The CEILING() function returns the smallest integer value that is bigger than or equal to a number.

> Note: This function is equal to the CEIL() function.


- Syntax

```sql
CEILING(number)
```

- Parameter Values


| Parameter | Description               |
|-----------|---------------------------|
| number    | Required. A numeric value |


- Examples

```sql
select ceiling(25.123);
```

## Floor

- Definition and Usage

The FLOOR() function returns the largest integer value that is smaller than or equal to a number.

> Note: Also look at the ROUND(), CEIL(), CEILING(), and DIV functions.

- Syntax

```sql
FLOOR(number)
```

- Parameter Values

| Parameter | Description               |
|-----------|---------------------------|
| number    | Required. A numeric value |


- Examples

```sql
select floor(25.8);
```

## Mod

- Definition and Usage

The MOD() function returns the remainder of a number divided by another number.

- Syntax

```sql
MOD(x, y)
```

- Parameter Values


| Parameter | Description                                 |
|-----------|---------------------------------------------|
| x         | Required. A value that will be divided by y |
| y         | Required. The divisor                       |

- Examples

```sql
select mod(5, 2);
```

## Abs

- Definition and Usage

The ABS() function returns the absolute (positive) value of a number.

- Syntax

```sql
ABS(number)
```

- Parameter Values


| Parameter | Description               |
|-----------|---------------------------|
| number    | Required. A numeric value |

- Examples

```sql
select abs(-10);
```