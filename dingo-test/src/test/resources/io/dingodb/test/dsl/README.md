# DSL cases file

## Table id naming Convention

| Column Types | Token |
|--------------|-------|
| TINYINT      | i1    |
| SMALLINT     | i2    |
| INT          | i4    |
| BIGINT       | i8    |
| FLOAT        | f4    |
| DOUBLE       | f8    |
| BOOL         | l     |
| DECIMAL      | dc    |
| CHAR         | s     |
| VARCHAR      | vs    |
| DATE         | dt    |
| TIME         | tm    |
| TIMESTAMP    | ts    |
| BINARY       | b     |
| VARBINARY    | vb    |
| MAP          | mp    |

# Rules

- If the type of column is nullable, add digit "0" after the token
- If the column is array, add "a" before the corresponding token
- If the column is primary key, add "k" after the token
- If columns of the same times occur multiple times continuously, the next token would be the times
- Finally, Concat all the tokens with "_" (underscore)

# Example

```sql
create table test (
    id int,
    name varchar(32) not null,
    amount double,
    primary key(id)
)
```

The table id of the above table is `i4k_vs_f80`.
