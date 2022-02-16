# Dingo Expression

Dingo Expression is the expression engine used by DingoDB. It is special for its runtime codebase is separated from the
parsing and compiling codebase. The classes in runtime are serializable so that they are suitable for runtime of
distributed computing system, like [Apache Flink](https://flink.apache.org/).

## Operators

| Category       | Operator                                     | Associativity |
| :------------- | :------------------------------------------- | :------------ |
| Parenthesis    | `( )`                                        |               |
| Function Call  | `( )`                                        | Left to right |
| Name Index     | `.`                                          | Left to right |
| Array Index    | `[ ]`                                        | Left to right |
| Unary          | `+` `-`                                      | Right to left |
| Multiplicative | `*` `/`                                      | Left to right |
| Additive       | `+` `-`                                      | Left to right |
| Relational     | `<` `<=` `>` `>=` `==` `=` `!=` `<>`         | Left to right |
| String         | `startsWith` `endsWith` `contains` `matches` | Left to right |
| Logical NOT    | `!` `not`                                    | Left to right |
| Logical AND    | `&&` `and`                                   | Left to right |
| Logical OR     | <code>&#x7c;&#x7c;</code> `or`               | Left to right |

## Data Types

| Type Name    | SQL type  | JSON Schema Type | Hosting Java Type      | Literal in Expression |
| :----------- | :-------- | :--------------- | :--------------------- | :-------------------- |
| Integer      | INTEGER   |                  | java.lang.Integer      |                       |
| Long         | LONG      | integer          | java.lang.Long         | `0` `20` `-375`       |
| Double       | DOUBLE    | number           | java.lang.Double       | `2.0` `-6.28` `3e-4`  |
| Boolean      | BOOLEAN   | boolean          | java.lang.Boolean      | `true` `false`        |
| String       | CHAR      | string           | java.lang.String       | `"hello"` `'world'`   |
| String       | VARCHAR   | string           | java.lang.String       | `"hello"` `'world'`   |
| Decimal      |           |                  | java.math.BigDecimal   |
| Time         | TIMESTAMP |                  | java.util.Date         |
| IntegerArray |           |                  | java.lang.Integer[]    |
| LongArray    |           | array            | java.lang.Long[]       |
| DoubleArray  |           | array            | java.lang.Double[]     |
| BooleanArray |           | array            | java.lang.Boolean[]    |
| StringArray  |           | array            | java.lang.String[]     |
| DecimalArray |           |                  | java.math.BigDecimal[] |
| ObjectArray  |           | array            | java.lang.Object[]     |
| List         |           | array            | java.util.List         |
| Map          |           | object           | java.util.Map          |
| Object       |           | object           | java.lang.Object       |

## Constants

| Name | Value                   |
| :--- | ----------------------: |
| TAU  | 6.283185307179586476925 |
| E    | 2.7182818284590452354   |

There is not "3.14159265" but `TAU`. See [The Tau Manifesto](https://tauday.com/tau-manifesto).

## Functions

### Mathematical

See [Math (Java Platform SE 8)](https://docs.oracle.com/javase/8/docs/api/java/lang/Math.html).

| Function  | Java function based on | Description |
| :-------- | :--------------------- | :---------- |
| `abs(x)`  | `java.lang.Math.abs`   |             |
| `sin(x)`  | `java.lang.Math.sin`   |             |
| `cos(x)`  | `java.lang.Math.cos`   |             |
| `tan(x)`  | `java.lang.Math.tan`   |             |
| `asin(x)` | `java.lang.Math.asin`  |             |
| `acos(x)` | `java.lang.Math.acos`  |             |
| `atan(x)` | `java.lang.Math.atan`  |             |
| `cosh(x)` | `java.lang.Math.cosh`  |             |
| `sinh(x)` | `java.lang.Math.sinh`  |             |
| `tanh(x)` | `java.lang.Math.tanh`  |             |
| `log(x)`  | `java.lang.Math.log`   |             |
| `exp(x)`  | `java.lang.Math.exp`   |             |

### Type conversion

| Function         | Java function based on | Description                          |
| :--------------- | :--------------------- | :----------------------------------- |
| `int(x)`         |                        | Convert `x` to Integer               |
| `long(x)`        |                        | Convert `x` to Long                  |
| `double(x)`      |                        | Convert `x` to Double                |
| `decimal(x)`     |                        | Convert `x` to Decimal               |
| `string(x)`      |                        | Convert `x` to String                |
| `string(x, fmt)` |                        | Convert `x` to String, `x` is a Time |
| `time(x)`        |                        | Convert `x` to Time                  |
| `time(x, fmt)`   |                        | Convert `x` to Time, `x` is a String |

### String

See [String (Java Platform SE 8)](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html).

| Function             | Java function based on | Description |
| :------------------- | :--------------------- | :---------- |
| `toLowerCase(x)`     | `String::toLowerCase`  |             |
| `toUpperCase(x)`     | `String::toUpperCase`  |             |
| `trim(x)`            | `String::trim`         |             |
| `replace(x, a, b)`   | `String::replace`      |             |
| `substring(x, s)`    | `String::substring`    |             |
| `substring(x, s, e)` | `String::substring`    |             |
