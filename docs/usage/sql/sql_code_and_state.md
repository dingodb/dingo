# SQLCODE and SQLSTATE

SQLCODE is an integer value returned by DingoDB driver indicating the error occurs during a database operation.

SQLSTATE is a code which identifies SQL error conditions. It composed by five characters, which can be numbers or
uppercase ASCII letters. An SQLSTATE value consists of a class (first two characters) and a subclass (last three
characters). The following SQLSTATE classes are defined in SQL standards:

* '00' means 'success'
* '01' contains all 'warnings'
* '02' is the 'NOT FOUND' class

SQLCODE and SQLSTATE of DingoDB

| SQLCODE | SQLSTATE | Description                      |
|---------|----------|----------------------------------|
| 42001   | 42001    | Connection refused               |
| 51001   | 51001    | SQL parse error                  |
| 51002   | 51002    | Illegal expression in context    |
| 52001   | 52001    | Unknown identifier               |
| 52002   | 52002    | Table already exists             |
| 52003   | 52003    | Primary keys are required        |
| 52004   | 52004    | Missing column list              |
| 53001   | 53001    | Table not found                  |
| 53002   | 53002    | Column not found                 |
| 53003   | 53003    | Value of column can not be NULL  |
| 54001   | 54001    | Illegal use of dynamic parameter |
| 60000   | 60000    | Task execution failed            |
| 90000   | 90000    | Unknown error                    |
