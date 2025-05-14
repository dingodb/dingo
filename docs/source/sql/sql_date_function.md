# Datetime Function
## List
| Function Type | Description  |Return Value Type|
|-----------|--------|--------|
| Now( )	|Returns the current datetime.	|Timestamp|
| CurDate( )	|Returns the current date.|	Date|
| Current_Date( )	|Returns the current date.|	Date|
| CurTime( )	|Returns the current time.|	Time|
| Current_Time( )	|Returns the current time.|	Time|
| Current_Timestamp( )	|Returns the current datetime.	|Timestamp|
| From_UnixTime(expr)	|Format Unix Timestamp.|	Timestamp|
| Unix_TimeStamp([expr])	|Returns the Unix timestamp.|	Long|
| Timestamp_format(timestamp,[format] )	|Format Time Date.|	String|
| Date_format(date,[format])	|Formats a date.|	String|
| Time_Fomat(time,[format])	|Formats a time.|	String|
| DateDiff（expr1，expr2）	|Finds the difference between two dates.|	Int|


## Specification
### Now

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

### CurDate

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

### Current_Date

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

### CurTime

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

### Current_Time

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

### Current_TimeStamp

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

### From_UnixTime

Format Unix Timestamp.

- Syntax

```
From_Unixtime(expr)
```

- Return Type

```
Return the processed result according to the date format specified by Format.The default format is: yyyy-MM-dd HH:mm:ss.
```

- Examples

```sql
0: jdbc:dingo::///> select From_Unixtime(1650968681);
+-------------------------+
|       EXPR$0            |
+-------------------------+
| 2022-04-26 18:24:41.000 |
+-------------------------+
```

### Unix_TimeStamp

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
### Timestamp_format(timestamp,[format] )
Format Time Date.
- Return Type

```
return the processed result according to the format defined by Format.
```

- Syntax
```
Timestamp_Format(timstamp,[format])
```

- Examples

```sql
0: jdbc:dingo::///> select Timestamp_Format('2022-07-11 18:00:00','%Y');
+--------+
| EXPR$0 |
+--------+
| 2022   |
+--------+
1 row selected (0.053 seconds)
0: jdbc:dingo::///> select Timestamp_Format('2022-07-11 18:00:00');
+---------------------+
|       EXPR$0           |
+---------------------+
| 2022-07-11T18:00:00 |
+---------------------+
```



### Date_Format

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
### Time_Fomat(time,[format])
Format time as specified.
- Syntax
```
Time_Format(time,[format])
```
- Return Type

```
Return the processed result according to the date format specified by Format.
```

- Examples
```
0: jdbc:dingo::///> select time_format('18:00:00');
+----------+
|  EXPR$0  |
+----------+
| 18:00:00 |
+----------+
1 row selected (0.035 seconds)
0: jdbc:dingo::///> select time_format('18:00:00','%H hour %i minutes %s second %f Microseconds ');
+----------------------------------------------+
|                    EXPR$0                    |
+----------------------------------------------+
| 18 hour 00 minutes 00 second f Microseconds  |
+----------------------------------------------+
1 row selected (0.017 seconds)
```

### DateDiff

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
