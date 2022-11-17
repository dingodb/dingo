# Dingo Expression

Dingo Expression is the expression engine used by DingoDB. It is special for its runtime codebase is separated from the
parsing and compiling codebase. The classes in runtime are serializable so that they are suitable for runtime of
distributed computing system, like [Apache Flink](https://flink.apache.org/).

## Getting started

Here is an example to use Dingo Expression.

```java
class Example1 {
    Object test() {
        // The original expression string.
        String exprString = "(1 + 2) * (5 - (3 + 4))";
        // parse it into an Expr object, the compiler can be reused.
        DingoExprCompiler compiler = new DingoExprCompiler();
        Expr expr = compiler.parse(exprString);
        // Compile in a CompileContext (can be null without variables in the expression) and get an RtExpr object.
        RtExpr rtExpr = expr.compileIn(null);
        // Evaluate it in an EvalContext (can be null without variables in the expression).
        return rtExpr.eval(null);
    }
}
```

## Operators

| Category       | Operator                                     | Associativity |
|:---------------|:---------------------------------------------|:--------------|
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

| Type Name | SQL type  | JSON Schema Type | Hosting Java Type                | Literal in Expression |
|:----------|:----------|:-----------------|:---------------------------------|:----------------------|
| NULL      | NULL      |                  | `null`                           | `null`                |
| INT       | INTEGER   |                  | `int` or `java.lang.Integer`     | `0` `20` `-375`       |
| LONG      | LONG      | integer          | `long` or `java.lang.Long`       |                       |
| DOUBLE    | DOUBLE    | number           | `double` or `java.lang.Double`   | `2.0` `-6.28` `3e-4`  |
| BOOL      | BOOLEAN   | boolean          | `boolean` or `java.lang.Boolean` | `true` `false`        |
| STRING    | VARCHAR   | string           | `java.lang.String`               | `"hello"` `'world'`   |
| BINARY    | BLOB      |                  | `byte[]`                         |
| DECIMAL   | DECIMAL   |                  | `java.math.BigDecimal`           |
| DATE      | DATE      |                  | `java.sql.Date`                  |
| TIME      | TIME      |                  | `java.sql.Time`                  |
| TIMESTAMP | TIMESTAMP |                  | `java.sql.Timestamp`             |
| OBJECT    | ANY       | object           | `java.lang.Object`               |
| ARRAY     |           |                  | `java.lang.Object[]`             |
| LIST      | ARRAY     | array            | `java.util.List`                 |
| MAP       | MAP       | object           | `java.util.Map`                  |

Dingo Expression parses integer literals adaptively, if an integer literal's value exceeds the range of Java `int` type,
it will be parsed to `LONG`; if the value exceeds the range of Java type `long`, it will be parsed to `DECIMAL`. On the
contrary, float literals are parsed to `DECIMAL` to keep the precision of value, you can then get a `DOUBLE` value by
casting functions.

## Three-valued logic

Dingo Expression has `NULL` type and support three-valued logic as in SQL. Generally, operators or functions evaluates
to `NULL` if any of its operands is `NULL` except some logical operators/functions.

## Constants

| Name |                   Value |
|:-----|------------------------:|
| TAU  | 6.283185307179586476925 |
| E    |   2.7182818284590452354 |

There is not "3.14159265" but `TAU`. See [The Tau Manifesto](https://tauday.com/tau-manifesto).

## Functions

Function names are case-insensitive.

### Casting

| Function       | Java function based on | Description              |
|:---------------|:-----------------------|:-------------------------|
| `int(x)`       |                        | Convert `x` to INT       |
| `long(x)`      |                        | Convert `x` to LONG      |
| `bool(x)`      |                        | Convert `x` to LONG      |
| `double(x)`    |                        | Convert `x` to DOUBLE    |
| `decimal(x)`   |                        | Convert `x` to DECIMAL   |
| `string(x)`    |                        | Convert `x` to STRING    |
| `date(x)`      |                        | Convert `x` to DATE      |
| `time(x)`      |                        | Convert `x` to TIME      |
| `timestamp(x)` |                        | Convert `x` to TIMESTAMP |
| `binary(x)`    |                        | Convert `x` to BINARY    |

**NOTE**: there are no `DATE`, `TIME` and `TIMESTAMP` literals, but you can write `TIMESTAMP('2020-12-21')`, etc.

### Logical

| Function          | Description                |
|:------------------|----------------------------|
| `and(a, b)`       | The same as `AND` operator |
| `or(a, b)`        | The same as `OR` operator  |
| `not(x)`          | The same as `NOT` operator |
| `is_true(x)`      | Test if `x` is `true`      |
| `is_not_true(x)`  | Test if `x` is not `true`  |
| `is_false(x)`     | Test if `x` is `false`     |
| `is_not_false(x)` | Test if `x` is not `false` |
| `is_null(x)`      | Test if `x` is `null`      |
| `is_not_null(x)`  | Test if `x` is not `null`  |

**NOTE**: `null` is not `true` or `false` and you can not test if a value is `null` by `x == null` because it
returns `null`.

### Mathematical

See [Math (Java Platform SE 8)](https://docs.oracle.com/javase/8/docs/api/java/lang/Math.html).

| Function    | Java function based on | Description           |
|:------------|:-----------------------|:----------------------|
| `min(a, b)` |                        | Min value of `a`, `b` |
| `max(a, b)` |                        | Max value of `a`, `b` |
| `abs(x)`    | `java.lang.Math.abs`   |                       |
| `sin(x)`    | `java.lang.Math.sin`   |                       |
| `cos(x)`    | `java.lang.Math.cos`   |                       |
| `tan(x)`    | `java.lang.Math.tan`   |                       |
| `asin(x)`   | `java.lang.Math.asin`  |                       |
| `acos(x)`   | `java.lang.Math.acos`  |                       |
| `atan(x)`   | `java.lang.Math.atan`  |                       |
| `cosh(x)`   | `java.lang.Math.cosh`  |                       |
| `sinh(x)`   | `java.lang.Math.sinh`  |                       |
| `tanh(x)`   | `java.lang.Math.tanh`  |                       |
| `log(x)`    | `java.lang.Math.log`   |                       |
| `exp(x)`    | `java.lang.Math.exp`   |                       |

### String

See [String (Java Platform SE 8)](https://docs.oracle.com/javase/8/docs/api/java/lang/String.html).

| Function           | Java function based on | Description |
|:-------------------|:-----------------------|:------------|
| `lower(s)`         | `String::toLowerCase`  |             |
| `upper(s)`         | `String::toUpperCase`  |             |
| `trim(s)`          | `String::trim`         |             |
| `replace(s, a, b)` | `String::replace`      |             |
| `substr(s, i)`     | `String::substring`    |             |
| `substr(s, i, j)`  | `String::substring`    |             |

## Variables and contexts

Variables can be used in expressions. In order to compile an expression containing variables, A `CompileContext` must be
provided to define the types of variables. A [JSON Schema](http://json-schema.org/) definition can be used as a source
of `CompileContext`. For example (in YAML format for simplicity, but you can surely use JSON format)

```yaml
type: object
properties:
    a:
        type: integer
    b:
        type: number
    c:
        type: boolean
    d:
        type: string
additionalProperties: false
```

where variables `a`, `b`, `c`, `d` are defined with specified types. According to the schema, you can provide a data
source as (in YAML format)

```yaml
{ a: 3, b: 4.0, c: false, d: bar }
```

Then an `RtExpr` can be compiled and evaluated as following

```java
class Example2 {
    Object test() {
        // jsonSchemaInYamlFormat is a String/InputStream contains the JSON Schema definition.
        RtSchemaRoot schemaRoot = SchemaParser.YAML.parse(jsonSchemaInYamlFormat);
        DingoExprCompiler compiler = new DingoExprCompiler();
        Expr expr = compiler.parse("a + b");
        RtExpr rtExpr = expr.compileIn(schemaRoot.getSchema());
        DataParser parser = DataParser.yaml(schemaRoot);
        // dataInYamlFormat is a String contains the JSON Schema definition.
        Object[] tuple = parser.parse(dataInYamlFormat);
        return rtExpr.eval(new TupleEvalContext(tuple));
    }
}
```

### Nested Context

In a JSON Schema definition, objects and arrays can be nested into each other, for example

```yaml
type: object
properties:
    a:
        type: object
        properties:
            b:
                type: number
            c:
                type: boolean
        additionalProperties: false
    d:
        type: array
        items:
            -   type: integer
            -   type: string
        additionalItems: false
additionalProperties: false
```

For JSON Schema of type `array`, if its `additionalItems` is set to `true`, the number and types of its elements are
determined, so each of its elements will be compiled to a standalone variable. This also happens to JSON Schema of
type `object` with a non-null `properties`, if its `properties` is not null and `additionalProperties` is set to `false`
, each of its properties will be compiled to a standalone variable. Otherwise, an `array` is compiled to `LIST` and
a `map` to `MAP`.

If an `object` type does not have `properties` defined, it is compiled to `OBJECT`.

As in the JSON schema above, you can use `a.b` and `a.c` to access the variables of `number` and `boolean` types,
separately. The syntax looks the same as map index, but they are really different variables and `a` is not an existing
variable at all in runtime. Also, you can use `d[0]` and `d[1]` to access the `integer` and the `string` variables
and `d` is not an existing variable.

The special variable `$` can be used to access the whole context, so `$.a` is the same as `a`. `$` is useful for a
context with an array as root. The parser also looks on `a.b` as `a['b']`, so the syntax to access variables is much
like JSONPath.
