# SQLCODE and SQLSTATE

SQLCODE is an integer value returned by DingoDB driver indicating the error occurs during a database operation.

SQLSTATE is a code which identifies SQL error conditions. It composed by five characters, which can be numbers or
uppercase ASCII letters. An SQLSTATE value consists of a class (first two characters) and a subclass (last three
characters). The following SQLSTATE classes are defined in SQL standards:

* '00' means 'success'
* '01' contains all 'warnings'
* '02' is the 'NOT FOUND' class

DingoDB try to make returned SQLCODE and SQLSTATE compatible with MySQL, if the error is not compatible with MySQL, its
SQLSTATE is "45000".

## SQLCODE and SQLSTATE of DingoDB

### MySQL compatible errors

| SQLCODE | SQLSTATE | Description                     |
|---------|----------|---------------------------------|
| 1152    | 08S01    | Connection refused              |
| 1064    | 42000    | SQL parse error                 |
| 1050    | 42S01    | Table already exists            |
| 4028    | HY000    | Missing column list             |
| 1146    | 42S02    | Table not found                 |
| 1054    | 42S22    | Column not found                |
| 1364    | HY000    | Value of column can not be NULL |

### DingoDB specified errors

| SQLCODE | SQLSTATE | Description                                                |
|---------|----------|------------------------------------------------------------|
| 1001    | 45000    | Primary keys are required                                  |
| 1002    | 45000    | Specified primary key does not exist in table              |
| 2001    | 45000    | Illegal use of dynamic parameter                           |
| 2002    | 45000    | Illegal expression in context                              |
| 2003    | 45000    | Unknown identifier                                         |
| 3001    | 45000    | Number format error                                        |
| 5001    | 45000    | Task execution failed                                      |
| 9001    | 45000    | Unknown error                                              |
| 9002    | 45000    | Intentional error thrown by function `throw` (for testing) |
