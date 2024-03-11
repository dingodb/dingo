<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    String charset = "utf8";
    String collate = "utf8_bin";
}
{
    ( <SCHEMA> | <DATABASE> ) ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ <CHARACTER> <SET> { charset = getNextToken().image; }]
    [ <COLLATE> { collate = getNextToken().image; } ]
    {
        return new io.dingodb.calcite.grammar.ddl.SqlCreateSchema(s.end(this), replace, ifNotExists, id, charset, collate);
    }
}

SqlCreate SqlCreateForeignSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNode type = null;
    SqlNode library = null;
    SqlNodeList optionList = null;
}
{
    <FOREIGN> <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
         <TYPE> type = StringLiteral()
    |
         <LIBRARY> library = StringLiteral()
    )
    [ optionList = Options() ]
    {
        return SqlDdlNodes.createForeignSchema(s.end(this), replace,
            ifNotExists, id, type, library, optionList);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    DingoSqlColumn columnDec;
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    SqlNodeList withColumnList = null;
    final Span s = Span.of();
    ColumnStrategy strategy = null;
    final String index;
    Boolean autoIncrement = false;
    Properties properties = null;
    PartitionDefinition partitionDefinition = null;
    int replica = 0;
    String engine = null;
    String indexType = "scalar";
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        [ <AUTO_INCREMENT> {autoIncrement = true; } ]
        nullable = NullableOptDefaultTrue()
        [ <AUTO_INCREMENT> {autoIncrement = true; } ]
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
          <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { strategy = ColumnStrategy.DEFAULT;}
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            columnDec = DingoSqlDdlNodes.createColumn(s.add(id).end(this), id, type.withNullable(nullable), e, strategy, autoIncrement);
            list.add(columnDec);
        }
        [ <PRIMARY> <KEY> { columnDec.setPrimaryKey(true); }]
        [ <COMMENT> (<IDENTIFIER>|<QUOTED_STRING>) { columnDec.setComment(token.image); } ]
        [ <ON> <UPDATE> <CURRENT_TIMESTAMP> ]
    )
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <INDEX> { s.add(this); }
        { index = getNextToken().image; }
        (
            <VECTOR>
            { indexType = "vector"; }
            columnList = ParenthesizedSimpleIdentifierList()
        |
            [<SCALAR>]
            columnList = ParenthesizedSimpleIdentifierList()
        )
        [ <WITH> withColumnList = ParenthesizedSimpleIdentifierList() ]
        [ <ENGINE> <EQ> { engine = getNextToken().image; if (engine.equalsIgnoreCase("innodb")) { engine = "TXN_LSM";} } ]
        [
           <PARTITION> <BY>
           {
               partitionDefinition = new PartitionDefinition();
               partitionDefinition.setFuncName(getNextToken().image);
               partitionDefinition.setColumns(readNames());
               partitionDefinition.setDetails(readPartitionDetails());
           }
        ]
        [
            <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
        ]
        [ <PARAMETERS> properties = readProperties() ]
        {
            list.add(new SqlIndexDeclaration(s.end(this), index, columnList, withColumnList, properties,
            partitionDefinition, replica, indexType, engine));
        }
    |
        <KEY> { s.add(this); } name = SimpleIdentifier()
        columnList = ParenthesizedSimpleIdentifierList()
        {
            index = name.getSimple();
            list.add(new SqlIndexDeclaration(s.end(this), index, columnList, withColumnList, properties,
            partitionDefinition, replica, indexType, null));
        }
    |
        <UNIQUE> { s.add(this); }
        (
          <KEY>
          |
          <INDEX>
        )?
        [ name = SimpleIdentifier() ]
        columnList = ParenthesizedSimpleIdentifierList() {
              list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
        [<USING> <IDENTIFIER> ]
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}
SqlCreate SqlCreateUser(Span s, boolean replace) :
{
    final String user;
    String password = "";
    String host = "%";
    SqlNode create = null;
    Boolean ifNotExists = false;
    String requireSsl = null;
    String lock = "N";
    Object expireDays = null;
}
{
    <USER> ifNotExists = IfNotExistsOpt()
    ( <QUOTED_STRING> | <IDENTIFIER> )
     { user = token.image; }
    [ <AT_SPLIT> (<QUOTED_STRING> | <IDENTIFIER>) { host = token.image; }  ]
    <IDENTIFIED> <BY>  <QUOTED_STRING> { password = token.image; }
    [ <REQUIRE> (  <SSL> { requireSsl = "SSL"; } | <NONE> { requireSsl  = "NONE"; }) ]
    [ <PASSWORD> <EXPIRE> { expireDays = "0"; } [ <INTERVAL> expireDays = number() <DAY> ] ]
    [ <ACCOUNT> [ <LOCK> { lock = "Y"; } ] [ <UNLOCK> { lock = "N"; } ] ]
    {
       return new SqlCreateUser(user, password, host, s.end(this), replace, ifNotExists, requireSsl, lock, expireDays);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
    int ttl = -1;
    PartitionDefinition partitionDefinition = null;
    int replica = 0;
    String engine = null;
    Properties properties = null;
    int autoIncrement = 1;
    String charset = "utf8";
    String collate = "utf8_bin";
    String comment = "";
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    (
     <ENGINE> <EQ> { engine = getNextToken().image; if (engine.equalsIgnoreCase("innodb")) { engine = "TXN_LSM";} }
     |
     <TTL> <EQ> [ <MINUS> {ttl = positiveInteger("-" + getNextToken().image, "ttl");} ]
        { ttl = positiveInteger(getNextToken().image, "ttl"); }
     |
       <PARTITION> <BY>
       {
           partitionDefinition = new PartitionDefinition();
           partitionDefinition.setFuncName(getNextToken().image);
           partitionDefinition.setColumns(readNames());
           partitionDefinition.setDetails(readPartitionDetails());
       }
    |
        <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
    |
     <WITH> properties = readProperties()
    |
     <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
     <AUTO_INCREMENT> <EQ> {autoIncrement = positiveInteger(getNextToken().image, "auto_increment"); }
    |
     <DEFAULT_>
    |
     <CHARSET> <EQ> { charset = getNextToken().image; }
    |
     <COLLATE> <EQ> { collate = getNextToken().image; }
    |
     <COMMENT> <EQ> { comment = getNextToken().image; }
    )*
    {
        return DingoSqlDdlNodes.createTable(
            s.end(this), replace, ifNotExists, id, tableElementList, query, ttl, partitionDefinition, replica,
            engine, properties, autoIncrement, comment, charset, collate
        );
    }
}

List<PartitionDetailDefinition> readPartitionDetails() : {
    List<PartitionDetailDefinition> partitionDetailDefinitions = new ArrayList<PartitionDetailDefinition>();
}{
    (
        <PARTITIONS>
        [
            <EQ>
            {
                partitionDetailDefinitions.add(new PartitionDetailDefinition(null, null, new Object[] {anything()}));
                return partitionDetailDefinitions;
            }
        ]
    |
        <VALUES> <LPAREN>
        { partitionDetailDefinitions.add(new PartitionDetailDefinition(null, null, readValues())); }
        <RPAREN>
        (
            LOOKAHEAD(2)
            <COMMA>
            <LPAREN>
            { partitionDetailDefinitions.add(new PartitionDetailDefinition(null, null, readValues())); }
            <RPAREN>
        )*
        { return partitionDetailDefinitions; }
    )
}

Object[] readValues() : {
   List<Object> values = new ArrayList<Object>();
}{
    { values.add(anything());}
    (
      <COMMA>
      { values.add(anything());}
    )*
     { return values.toArray(); }
}

List<String> readNames()  : {
	List<String> names = new ArrayList<String>();
} {
      [
	  <LPAREN>
        {names.add(getNextToken().image);}
	    (
	      <COMMA>
          {names.add(getNextToken().image);}
	    )*
	  <RPAREN>
      ]
	{ return names; }
}

Properties readProperties() : {
	final Properties properties = new Properties();
	String key = null;
}{
	<LPAREN>
    [<RPAREN> {return properties;}]
    { key = getNextToken().image; }
    <EQ>
    { properties.setProperty(key, getNextToken().image); }
    (
        <COMMA>
        { key = getNextToken().image; }
        <EQ>
        { properties.setProperty(key, getNextToken().image); }
    )*
	<RPAREN>
	{ return properties; }
}

String symbol() : {
}{
	<IDENTIFIER>
	{ return token.image; }
}

Object nullValue(): {}{
	<NULL>
	{ return null; }
}

Object anything() : {
	Object x;
}{
	(
	  x = symbol()
	| <DECIMAL_NUMERIC_LITERAL>
	| <DATE_LITERAL>
	| <TIME_LITERAL>
	| <DATE_TIME>
    | <QUOTED_STRING> {return SqlParserUtil.parseString(token.image);}
	| x = number() { return x; }
	| x = booleanValue()
	| x = NonReservedKeyWord()
	| x = nullValue()
	)
	{ return token.image; }
}

Boolean booleanValue(): {
	Boolean b;
}{
	(
		(
			<TRUE>
			{ b = Boolean.TRUE; }
		) | (
			<FALSE>
			{ b = Boolean.FALSE; }
		)
	)
	{ return b; }
}

Number number(): {
	Token t;
	Number n;
}{
	 (
        t = <UNSIGNED_INTEGER_LITERAL>
        {
                    return new BigInteger(t.image);
        }
      ) | (
          t = <DECIMAL_NUMERIC_LITERAL>
          {

                  return new Double(t.image);
          }
       ) | (
         <MINUS>
         n = number() {
            if (n instanceof BigInteger) {
               BigInteger val = (BigInteger)n;
               return val.multiply(new BigInteger("-1"));
            } else if (n instanceof Double) {
               Double val = (Double)n;
               return val * -1;
            } else {
               int val = n.intValue();
               return val * -1;
            }
          }
      )
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, columnList,
            query);
    }
}

SqlCreate SqlCreateMaterializedView(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <MATERIALIZED> <VIEW> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createMaterializedView(s.end(this), replace,
            ifNotExists, id, columnList, query);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
        <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
    |
        <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
    |
        <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), replace, ifNotExists,
            id, className, usingList);
    }
}

SqlCreate SqlCreateIndex(Span s, boolean replace) :
{
    boolean isUnique = false;
    final String index;
    SqlIdentifier table;
    SqlIdentifier column;
    List<SqlIdentifier> columns;
    SqlNode create = null;
    Boolean ifNotExists = false;
}
{
    [ <UNIQUE> { isUnique = true;}]
    <INDEX> ifNotExists = IfNotExistsOpt()
    ( <QUOTED_STRING> | <IDENTIFIER> )
     { index = token.image.toUpperCase(); }
    <ON> table = CompoundIdentifier()
    <LPAREN> column = SimpleIdentifier() { columns = new ArrayList<SqlIdentifier>(); columns.add(column); }
    (
       <COMMA>
       column = SimpleIdentifier() { columns.add(column); }
    )*
    <RPAREN>
    {
       return new SqlCreateIndex(s.end(this), replace, ifNotExists, index, table, columns, isUnique);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final boolean foreign;
}
{
    (
        <FOREIGN> { foreign = true; }
    |
        { foreign = false; }
    )
    ( <SCHEMA> | <DATABASE> ) ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), foreign, ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropUser(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier name;
    String user;
    String host = "%";
}
{
    <USER> ifExists = IfExistsOpt()
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { user = token.image; }
    [ <AT_SPLIT> (<QUOTED_STRING> | <IDENTIFIER> ) { host = token.image;} ]
    {
        return new SqlDropUser(s.end(this), ifExists, user, host);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropMaterializedView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <MATERIALIZED> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropMaterializedView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropIndex(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final String index;
}
{
    <INDEX> ifExists = IfExistsOpt()
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { index = token.image.toUpperCase(); }
    <ON>
    id = CompoundIdentifier()
    {
      return new SqlDropIndex(s.end(this), ifExists, index, id);
    }
}

SqlIdentifier getSchemaTable() :
{
    final List<String> nameList = new ArrayList<String>();
    final List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
    boolean star = false;
}
{
    schemaTableSegment(nameList, posList)
    (
        LOOKAHEAD(2)
        <DOT>
        schemaTableSegment(nameList, posList)
    )*
    (
        LOOKAHEAD(2)
        <DOT>
        <STAR> {
            star = true;
            nameList.add("");
            posList.add(getPos());
        }
    )?
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        if (star) {
            return SqlIdentifier.star(nameList, pos, posList);
        }
        return new SqlIdentifier(nameList, null, pos, posList);
    }
}

void schemaTableSegment(List<String> names, List<SqlParserPos> positions) :
{
    final String id;
    char unicodeEscapeChar = BACKSLASH;
    final SqlParserPos pos;
    final Span span;
}
{
    (
        <IDENTIFIER> {
            id = unquotedIdentifier();
            pos = getPos();
        }
    |
        <HYPHENATED_IDENTIFIER> {
            id = unquotedIdentifier();
            pos = getPos();
        }
    |
        <QUOTED_IDENTIFIER> {
            id = SqlParserUtil.stripQuotes(getToken(0).image, DQ, DQ, DQDQ,
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BACK_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.stripQuotes(getToken(0).image, "`", "`", "``",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BIG_QUERY_BACK_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.stripQuotes(getToken(0).image, "`", "`", "\\`",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BRACKET_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.stripQuotes(getToken(0).image, "[", "]", "]]",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <UNICODE_QUOTED_IDENTIFIER> {
            span = span();
            String image = getToken(0).image;
            image = image.substring(image.indexOf('"'));
            image = SqlParserUtil.stripQuotes(image, DQ, DQ, DQDQ, quotedCasing);
        }
        [
            <UESCAPE> <QUOTED_STRING> {
                String s = SqlParserUtil.parseString(token.image);
                unicodeEscapeChar = SqlParserUtil.checkUnicodeEscapeChar(s);
            }
        ]
        {
            pos = span.end(this).withQuoting(true);
            SqlLiteral lit = SqlLiteral.createCharString(image, "UTF16", pos);
            lit = lit.unescapeUnicode(unicodeEscapeChar);
            id = lit.toValue();
        }
    |
        id = NonReservedKeyWord() {
            pos = getPos();
        }
    |
        <STAR> {
         id = "*";
         pos = getPos();
        }
    )
    {
        if (id.length() > this.identifierMaxLength) {
            throw SqlUtil.newContextException(pos,
                RESOURCE.identifierTooLong(id, this.identifierMaxLength));
        }
        names.add(id);
        if (positions != null) {
            positions.add(pos);
        }
    }
}

SqlDesc SqlDesc(): {
    final Span s;
    SqlIdentifier tableName = null;
} {
    <DESC> { s = span(); }
    tableName = CompoundTableIdentifier()
    { return new SqlDesc(s.end(this), tableName); }
}

SqlNode ScopeVariable(): {
       final SqlFunctionCategory funcType;
       final SqlIdentifier qualifiedName;
       final Span s;
       SqlCharStringLiteral literal;
       final SqlIdentifier id;
       final List<SqlNode> args = new ArrayList();
       SqlLiteral quantifier = null;
} {
   (
     <AT_SPLIT>
         {
         qualifiedName = new SqlIdentifier("@", getPos());
         }
   |
     <AT_SPLIT_2>
         {
         qualifiedName = new SqlIdentifier("@@", getPos());
         }
   )
     id = CompoundIdentifier()
     {
         String p = null;
         if (id.names.size() == 1) {
             p = id.names.get(0);
         } else if (id.names.size() == 2) {
             p = id.names.get(0) + "." + id.names.get(1);
         }
         literal = SqlLiteral.createCharString(p, getPos());
         args.add(literal);

         funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
         s = span();
         quantifier = null;

         return createCall(qualifiedName, s.end(this), funcType, quantifier, args);
     }
}

SqlCommit SqlCommit(): {
   Span s;
} {
   <COMMIT> { s = span(); return new SqlCommit(s.end(this)); }
}

SqlRollback SqlRollback(): {
   Span s;
} {
  <ROLLBACK> { s = span(); return new SqlRollback(s.end(this)); }
}

SqlUseSchema SqlUseSchema(): {
   Span s;
} {
  <USE> <IDENTIFIER> { s = span(); return new SqlUseSchema(s.end(this), token.image); }
}

SqlPrepare SqlPrepare(): {
   Span s;
   String statementName;
   String prepareSql;
} {
  <PREPARE> <IDENTIFIER> { s = span(); statementName = token.image; }
  <FROM> <QUOTED_STRING> { prepareSql = token.image; return new SqlPrepare(s.end(this), statementName, prepareSql); }
}

SqlExecute SqlExecute(): {
   Span s;
   String statementName;
   List<String> paramList = new ArrayList();
} {
   <EXECUTE> <IDENTIFIER> { s = span(); statementName = token.image; }
   <USING>
   <AT_SPLIT> <IDENTIFIER> { paramList.add(token.image); }
   (
     <COMMA> <AT_SPLIT> <IDENTIFIER> { paramList.add(token.image); }
   )*
   {
      return new SqlExecute(s.end(this), statementName, paramList);
   }
}

SqlAlterUser SqlAlterUser(Span s, String scope): {
    final String user;
    String password = null;
    String host = "%";
    SqlNode create = null;
    String requireSsl = null;
    String lock = null;
    Object expireDays = null;
} {
   <USER>
   ( <QUOTED_STRING> | <IDENTIFIER> )
     { s = span(); user = token.image; }
    [ <AT_SPLIT> (<QUOTED_STRING> | <IDENTIFIER>) { host = token.image; }  ]
    [ <IDENTIFIED> <BY>  <QUOTED_STRING> { password = token.image; } ]
    [ <REQUIRE> (  <SSL> { requireSsl = "SSL"; } | <NONE> { requireSsl  = "NONE"; }) ]
    [ <PASSWORD> <EXPIRE> { expireDays = "0"; } [ <INTERVAL> expireDays = number() <DAY> ] ]
    [ <ACCOUNT> [ <LOCK> { lock = "Y";} ] [ <UNLOCK> { lock = "N"; } ] ]
    {
      return new SqlAlterUser(user, password, host, requireSsl, s.end(this), lock, expireDays);
    }
}

/*
 * Sql Truncate
*/

SqlNode SqlTruncate() :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TRUNCATE>
    [
      <TABLE>
    ]
    id = CompoundIdentifier() {
        return new SqlTruncate(getPos(), id);
    }
}

SqlNode TableFunctionCall() :
{
    final Span s;
    final SqlNode call;
    SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    (
        <TABLE> { s = span(); } <LPAREN>
        [
            <SPECIFIC>
        {
            funcType = SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION;
        }
        ]
        call = NamedRoutineCall(funcType, ExprContext.ACCEPT_CURSOR)
        <RPAREN>
        {
            return SqlStdOperatorTable.COLLECTION_TABLE.createCall(s.end(this), call);
        }
    |
        <SCAN> { s = span(); } <LPAREN>
        [
            AddArg0(list, ExprContext.ACCEPT_CURSOR)
            (
                <COMMA> {
                    // a comma-list can't appear where only a query is expected
                    checkNonQueryExpression(ExprContext.ACCEPT_CURSOR);
                }
                AddArg(list, ExprContext.ACCEPT_CURSOR)
            )*
        ]
        <RPAREN>
        {
            return SqlUserDefinedOperators.SCAN.createCall(s.end(this), list);
        }
    |
        <VECTOR> { s = span(); } <LPAREN>
        [
            AddArg0(list, ExprContext.ACCEPT_CURSOR)
            (
                <COMMA> {
                    // a comma-list can't appear where only a query is expected
                    checkNonQueryExpression(ExprContext.ACCEPT_CURSOR);
                }
                AddArg(list, ExprContext.ACCEPT_CURSOR)
            )*
        ]
        <RPAREN>
        {
            return SqlUserDefinedOperators.VECTOR.createCall(s.end(this), list);
        }
    )
}
