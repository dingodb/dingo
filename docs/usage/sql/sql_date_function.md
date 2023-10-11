# Datetime Function

## Now

 Return the current date and time.

- Syntax

```
Now()
```

- Return Type

```
Timestamp,Returns a date in the format 'yyyy-MM-dd HH:mm:ss.sss'.
```

- Examples

```sql
dingo>  select now();
+-------------------------+
| EXPR$0                  |
+-------------------------+
| 2022-04-26 08:14:00.384 |
+-------------------------+
```

## CurDate

Return the current date.

- Syntax

```
CurDate()
```

- Return Type

```
Date, Returns a date in the format 'yyyy-MM-dd'.
```

- Examples

```sql
dingo> select CurDate();
+------------+
|   EXPR$0   |
+------------+
| 2022-04-26 |
+------------+
```

## Current_Date

Return the current date.

- Syntax

```
Current_Date()
```

- Return Type

```
Returns a date in the format 'yyyy-MM-dd'.
```

- Examples

```sql
dingo> select Current_Date();
+------------+
|   EXPR$0   |
+------------+
| 2022-04-26 |
+------------+
```

## CurTime

Return the current time.

- Syntax

```
CurTime()
```

- Return Type

```
Returns the time in 'HH:mm:ss.sss' format.
```

- Examples

```sql
dingo> Select CurTime();
+---------------+
|  EXPR$0       |
+---------------+
| 16:56:43.342  |
+---------------+
```

## Current_Time

Return the current time.

- Syntax

```
Current_Time()
```

- Return Type

```
Returns the time in 'HH:mm:ss.sss' format.
```

- Examples

```sql
dingo> Select Current_Time();
+--------------+
|  EXPR$0      |
+--------------+
| 17:00:41.532 |
+--------------+
```

## Current_TimeStamp

Return the current date and time.

- Syntax

```
Current_TimeStamp()
```

- Return Type

```
Returns the date and time in the format "yyyy-MM-ss HH:mm:ss.sss".
```

- Examples

```sql
dingo> Select Current_TimeStamp();
+-------------------------+
|   EXPR$0                |
+-------------------------+
| 2022-04-26 17:01:43.123 |
+-------------------------+
```

## Form_UnixTime

```
Format Unix timestamp as a date.
```

- Syntax

```
Form_UnixTime(unix_timestamp)
```

- Return Type

```
Return the processed result according to the date format specified by Format.The default format is: yyyy-MM-dd HH:mm:ss.
```

- Examples

```sql
dingo> Select From_UnixTime(1650968681);
+-------------------------+
|       EXPR$0            |
+-------------------------+
| 2022-04-26 18:24:41.000 |
+-------------------------+
```

## Unix_TimeStamp

Return a Unix timestamp.

- Syntax

```
Unix_TimeStamp([expr])
```

- Parameter

```
+------------+-----------------+------------------------+
| parameter  | paratemer type  |  example               |
+------------+-----------------+------------------------+
|            | Timestamp       | 2000-01-01 12:00:00.000|
|     expr   |-----------------+------------------------+
|            | Long            | 1646591400             |
+------------+-----------------+------------------------+
```

- Return Type

```
If UNIX_TIMESTAMP() is called without a date argument, returns the timestamp of the current time.
```

- Examples

```sql
dingo> Select Unix_TimeStamp();
+------------+
|   EXPR$0   |
+------------+
| 1651025382 |
+------------+

dingo> Select Unix_TimeStamp('20220427');
+------------+
|   EXPR$0   |
+------------+
| 1650988800 |
+------------+

dingo> Select Unix_TimeStamp('2022-04-27');
+------------+
|   EXPR$0   |
+------------+
| 1650988800 |
+------------+

dingo> Select Unix_TimeStamp('2022/04/27');
+------------+
|   EXPR$0   |
+------------+
| 1650988800 |
+------------+

dingo> Select Unix_TimeStamp('2022.04.27');
+------------+
|   EXPR$0   |
+------------+
| 1650988800 |
+------------+

dingo> Select Unix_TimeStamp(1650988800);
+------------+
|   EXPR$0   |
+------------+
| 1650988800 |
+------------+
```

## Date_Format

Format date as specified.

- Syntax

```
Date_Format(date,format)
```

- Return Type

```
Return the processed result according to the date format specified by Format.
```

- Examples

```sql
dingo> Select Date_Format('2022-04-27','%Y/%m/%d');
+------------+
|   EXPR$0   |
+------------+
| 2022/04/27 |
+------------+
```

## DateDiff

```
Subtract two dates.
```

- Syntax

```
DateDiff(expr1,expr2)
```

- Return Type

```
Int
```

- Examples

```sql
dingo>Select DateDiff('2022-11-15','2021-05-03');
+--------+
| EXPR$0 |
+--------+
| 561    |
+--------+
```
