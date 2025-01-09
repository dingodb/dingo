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

SqlAlter SqlRenameTable(): {
  Span s;
  SqlIdentifier id = null;
  SqlIdentifier toId = null;
  List<SqlIdentifier> originIdList = new ArrayList<>();
  List<SqlIdentifier> toIdList = new ArrayList<>();
} {
   <RENAME> { s = span(); }
   <TABLE> { s.add(this); }
   id = CompoundIdentifier()
   <TO>
   toId = CompoundIdentifier()
   {
     originIdList.add(id);
     toIdList.add(toId);
   }
   (
     <COMMA>
     id = CompoundIdentifier()
     <TO>
     toId = CompoundIdentifier()
     {
       originIdList.add(id);
       toIdList.add(toId);
     }
   )*
   {
     return new SqlAlterRenameTable(s.end(this), originIdList, toIdList);
   }
}

SqlAlterSchema SqlAlterSchema(Span s, String scope): {
  SqlIdentifier id = null;
  String charset = null;
  String collate = null;
} {
   (<SCHEMA>|<DATABASE>) { s.add(this); } id = CompoundIdentifier()
   (
    <DEFAULT_>
    |
    <CHARACTER> <SET> <EQ> { charset = getNextToken().image; }
    |
    <COLLATE> <EQ> { collate = getNextToken().image; }
   )*
   {
     return new SqlAlterSchema(s.end(this), id, charset, collate);
   }
}

SqlAlterTable SqlAlterIgnoreTable(Span s, String scope): {
    SqlAlterTable alterTable = null;
} {
  <IGNORE>
  alterTable = SqlAlterTable(s, scope)
  {
    return alterTable;
  }
}

SqlAlterTable SqlAlterTable(Span s, String scope): {
    SqlIdentifier id;
    SqlAlterTable alterTable = null;
} {
    <TABLE> id = CompoundIdentifier()
    (
	    <ADD>
	    (
	        alterTable = addPartition(s, scope, id)
	    |
	        alterTable = addIndex(s, scope, id)
        |
            alterTable = addUniqueIndex(s, scope, id)
        |
            alterTable = addColumn(s, scope, id)
        |
            alterTable = addConstraint(s, scope, id)
        |
            <FULLTEXT> alterTable = addIndexByMode(s, scope, id, "fulltext")
        |
           <SPATIAL> alterTable = addIndexByMode(s, scope, id, "spetail")
	    )
	  |
         <DROP>
         (
           alterTable = dropIndex(s, scope, id)
           |
           alterTable = dropColumn(s, scope, id)
           |
           alterTable = dropConstraint(s, scope, id)
           |
           alterTable = dropCheck(s, scope, id)
           |
           alterTable = dropForeignKey(s, scope, id)
           |
           alterTable = dropPartition(s, scope, id)
         )
        |
         <TRUNCATE>
          alterTable = alterTableTruncatePart(s, scope, id)
        |
          <MODIFY>
          alterTable = modifyColumn(s, scope, id, alterTable)
          (
            <COMMA>
            <MODIFY> alterTable = modifyColumn(s, scope, id, alterTable)
          )*
        |
          <CHANGE>
          alterTable = changeColumn(s, scope, id)
        |
         <EXCHANGE> alterTable = alterExchange(s, scope, id)
        |
         <AUTO_INCREMENT> alterTable = alterAutoInc(s, scope, id)
        |
         <RENAME> alterTable = alterRenameIndex(s, scope, id)
        |
         <COMMENT> alterTable = alterTableComment(s, scope, id)
        |
        <ALTER>
         (
          alterTable = alterIndex(s, scope, id)
          |
          alterTable = alterConstraint(s, scope, id)
          |
          alterTable = alterColumn(s, scope, id)
         )
        |
	<CONVERT> <TO>
	alterTable = convertCharset(s, id)
    )
    { return alterTable; }
}

SqlAlterTable addPartition(Span s, String scope, SqlIdentifier id): {
   String pName = null;
   Object[] values = null;
} {
    (
    <DISTRIBUTION> <BY>
    {
        return new SqlAlterTableDistribution(s.end(this), id, readPartitionDetails().get(0));
    }
    |
    <PARTITION> pName=dingoIdentifier()
    <VALUES> <LESS> <THAN>
    <LPAREN>
     values = readValues()
    <RPAREN>
    {
      PartitionDetailDefinition newPart = new PartitionDetailDefinition(pName, null, values);
      return new SqlAlterTableAddPart(s.end(this), id, newPart);
    }
    )
}

SqlAlterTable addColumn(Span s, String scope, SqlIdentifier id): {
    final SqlIdentifier columnId;
    final SqlDataTypeSpec type;
    SqlIdentifier afterCol = null;
    ColumnOption columnOpt = null;
} {
    <COLUMN>
    columnId = SimpleIdentifier()
    type = DataType()
    columnOpt = parseColumnOption()
    [
          (
           <AFTER> afterCol = SimpleIdentifier()
           |
           <FIRST>
          )
    ]
    {
        boolean nullable = true;
        if (columnOpt != null) { nullable = columnOpt.nullable; }
        return new SqlAlterAddColumn(s.end(this), id, DingoSqlDdlNodes.createColumn(
            s.end(this), columnId, type.withNullable(nullable), columnOpt
        ));
    }
}

SqlAlterTable dropIndex(Span s, String scope, SqlIdentifier id): {
  String index;
} {
   <INDEX> { s.add(this); }
   { index = getNextToken().image; }
   {
     return new SqlAlterDropIndex(s.end(this), id, index);
   }
}

SqlAlterTable dropColumn(Span s, String scope, SqlIdentifier id): {
  String column;
} {
   <COLUMN> { s.add(this); }
   { column = getNextToken().image; }
   {
     return new SqlAlterDropColumn(s.end(this), id, column);
   }
}


SqlAlterTable addIndex(Span s, String scope, SqlIdentifier id): {
    final String index;
    Boolean autoIncrement = false;
    Properties properties = null;
    PartitionDefinition partitionDefinition = null;
    int replica = 0;
    String indexType = "scalar";
    SqlNodeList withColumnList = null;
    List<SqlNode> columnList;
    String engine = null;
    Properties prop = new Properties();
    String indexAlg = null;
    String indexLockOpt = null;
    boolean ifNotExists = false;
} {
 (<INDEX>|<KEY>) ifNotExists = IfNotExistsOpt() { s.add(this); }
    { index = getNextToken().image; }
    (
        <VECTOR> { indexType = "vector"; } columnList = indexColumns()
    |
        <TEXT> { indexType = "text"; } columnList = indexColumns()
    |
        [<SCALAR>] columnList = indexColumns()
    )
    (
       <WITH> withColumnList = ParenthesizedSimpleIdentifierList()
     |
       <ENGINE> <EQ> engine = dingoIdentifier() { if (engine.equalsIgnoreCase("innodb")) { engine = "TXN_LSM";} }
     |
        <PARTITION> <BY>
            {
                partitionDefinition = new PartitionDefinition();
                partitionDefinition.setFuncName(getNextToken().image);
                partitionDefinition.setDetails(readPartitionDetails());
            }
     |
        <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
     |
        <PARAMETERS> properties = readProperties()
    )*
    prop = indexOption()
    [ indexAlg = indexAlg()]
    [ indexLockOpt = indexLockOpt()]
    {
        return new SqlAlterAddIndex(
            s.end(this), id,
            new SqlIndexDeclaration(
                s.end(this), index, columnList, withColumnList, properties,partitionDefinition, replica, indexType, engine, "", prop, indexAlg, indexLockOpt
            )
        );
    }
}

SqlAlterTable addUniqueIndex(Span s, String scope, SqlIdentifier id): {
    final String index;
    Boolean autoIncrement = false;
    Properties properties = null;
    PartitionDefinition partitionDefinition = null;
    int replica = 0;
    String indexType = "scalar";
    SqlNodeList withColumnList = null;
    List<SqlNode> columnList;
    String engine = null;
    Properties prop = null;
    String indexAlg = null;
    String indexLockOpt = null;
} {
 <UNIQUE> [<INDEX>][<KEY>] { s.add(this); }
    { index = getNextToken().image; }
    [<SCALAR>] columnList = indexColumns()
    (
       <WITH> withColumnList = ParenthesizedSimpleIdentifierList()
     |
       <ENGINE> <EQ> engine = dingoIdentifier() { if (engine.equalsIgnoreCase("innodb")) { engine = "TXN_LSM";} }
     |
        <PARTITION> <BY>
            {
                partitionDefinition = new PartitionDefinition();
                partitionDefinition.setFuncName(getNextToken().image);
                partitionDefinition.setDetails(readPartitionDetails());
            }
     |
        <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
     |
        <PARAMETERS> properties = readProperties()
    )*
    prop = indexOption()
    [ indexAlg = indexAlg()]
    [ indexLockOpt = indexLockOpt()]
    {
        return new SqlAlterAddIndex(
            s.end(this), id,
            new SqlIndexDeclaration(
                s.end(this), index, columnList, withColumnList, properties,partitionDefinition, replica, indexType, engine, "unique", prop, indexAlg, indexLockOpt
            )
        );
    }
}

SqlAlterTable addIndexByMode(Span s, String scope, SqlIdentifier id, String mode): {
    final String index;
    Boolean autoIncrement = false;
    Properties properties = null;
    PartitionDefinition partitionDefinition = null;
    int replica = 0;
    String indexType = "scalar";
    SqlNodeList withColumnList = null;
    List<SqlNode> columnList;
    String engine = null;
    Properties prop = null;
    String indexAlg = null;
    String indexLockOpt = null;
} {
   [(<INDEX>|<KEY>)] { s.add(this); }
    { index = getNextToken().image; }
    columnList = indexColumns()
    (
       <WITH> withColumnList = ParenthesizedSimpleIdentifierList()
     |
       <ENGINE> <EQ> engine = dingoIdentifier() { if (engine.equalsIgnoreCase("innodb")) { engine = "TXN_LSM";} }
     |
        <PARTITION> <BY>
            {
                partitionDefinition = new PartitionDefinition();
                partitionDefinition.setFuncName(getNextToken().image);
                partitionDefinition.setDetails(readPartitionDetails());
            }
     |
        <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
     |
        <PARAMETERS> properties = readProperties()
    )*
    prop = indexOption()
    [ indexAlg = indexAlg()]
    [ indexLockOpt = indexLockOpt()]
    {
        return new SqlAlterAddIndex(
            s.end(this), id,
            new SqlIndexDeclaration(
                s.end(this), index, columnList, withColumnList, properties,partitionDefinition, replica, indexType, engine, mode, prop, indexAlg, indexLockOpt
            )
        );
    }
}


SqlAlterTable alterIndex(Span s, String scope, SqlIdentifier id): {
    final String index;
    Properties properties = new Properties();
    String key;
    boolean visible = true;
}
{
    <INDEX> { s.add(this); }
    { index = getNextToken().image; }
    (
    <SET>
    readProperty(properties)
    (
        <COMMA>
        readProperty(properties)
    )*
    (
        <AND>
        readProperty(properties)
    )*
    {
        return new SqlAlterIndex(
            s.end(this), id, index, properties
        );
    }
    |
     <INVISIBLE> { visible = false; return new SqlAlterIndexVisible(s.end(this), id, index, visible); }
    |
     <VISIBLE> { visible = true; return new SqlAlterIndexVisible(s.end(this), id, index, visible); }
    )
}

SqlAlterTable convertCharset(Span s, SqlIdentifier id): {
    final String charset;
    String collate = "utf8_bin";
} {
  <CHARACTER> <SET> { charset = this.getNextToken().image; } [<COLLATE> { collate = this.getNextToken().image; }]
  { return new SqlAlterConvertCharset(s.end(this), id, charset, collate); }
}

SqlAlterTable addConstraint(Span s, String scope, SqlIdentifier id): {
   SqlIdentifier name = null;
   SqlNode e = null;
   boolean enforced = false;
   SqlNodeList columnList = null;
   String index = null;
   SqlAlterAddForeign sqlAlterAddForeign = null;
   Properties prop = null;
   String indexAlg = null;
   boolean checkNot = false;
   String indexLockOpt = null;
} {
    <CONSTRAINT> { s.add(this); } [ name = SimpleIdentifier() ]
    (
     <CHECK> <LPAREN>
     e = Expression(ExprContext.ACCEPT_SUB_QUERY)
     <RPAREN>
     (<NOT>|{checkNot = false;}) (<ENFORCED>|<NULL>|{ String t = "";})
     {
        return new SqlAlterAddConstraint(s.end(this), name, e, enforced);
     }
    |
      <UNIQUE> [<KEY>] [<INDEX>] { index = getNextToken().image; }
      [ indexTypeName() ]
      columnList = ParenthesizedSimpleIdentifierList()
      prop = indexOption()
      [ indexAlg = indexAlg()]
      [ indexLockOpt = indexLockOpt()]
      {
        return new SqlAlterAddIndex(
            s.end(this), id,
            new SqlIndexDeclaration(
                s.end(this), index, columnList, null, null,null, 3, "scalar", null, true
            )
         );
      }
    |
      sqlAlterAddForeign = foreign(s, id)
      {
         return sqlAlterAddForeign;
      }
    )
}

SqlAlterAddForeign foreign(Span s, SqlIdentifier id): {
   SqlNodeList columnList = null;
   SqlNodeList refColumnList = null;
   SqlIdentifier refTable = null;
   SqlIdentifier name = null;
   String updateRefOpt = null;
   String deleteRefOpt = null;
} {
   <FOREIGN><KEY> [ name = SimpleIdentifier() ]
   columnList = ParenthesizedSimpleIdentifierList()
   <REFERENCES> refTable = CompoundIdentifier() refColumnList = ParenthesizedSimpleIdentifierList()
   [<MATCH>(<FULL>|<PARTIAL>|<SIMPLE>)]
   ( <ON> (
           <UPDATE> updateRefOpt = referenceOpt()
           |
           <DELETE> deleteRefOpt = referenceOpt()
          )
   )*
   {
      return new SqlAlterAddForeign(s.end(this), id, name, columnList, refTable, refColumnList, updateRefOpt, deleteRefOpt);
   }
}

String referenceOpt(): {
  String refOpt = null;
} {
  (
   <RESTRICT> { refOpt = "restrict"; }
   |
   <CASCADE> { refOpt = "cascade"; }
   |
   <SET>
    (
      <NULL> { refOpt = "setNull"; }
      |
      <DEFAULT_> { refOpt = "setDefault"; }
    )
   |
   <NO> <ACTION> { refOpt = "noAction"; }
  )
  {
    return refOpt;
  }
}

SqlAlterTable dropConstraint(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
} {
   <CONSTRAINT> { s.add(this); } name = SimpleIdentifier()
   {
     return new SqlAlterDropConstraint(s.end(this), name);
   }
}

SqlAlterTable dropCheck(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
} {
   <CHECK> { s.add(this); } name = SimpleIdentifier()
   {
     return new SqlAlterDropConstraint(s.end(this), name);
   }
}


SqlAlterTable alterConstraint(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
  boolean enforced = false;
} {
  <CONSTRAINT> { s.add(this); } name = SimpleIdentifier()
  (
    <NOT> <ENFORCED> { enforced = false; }
   |
    <ENFORCED> { enforced = true; }
  )
  {
    return new SqlAlterConstraint(s.end(this), name, enforced);
  }
}

SqlAlterTable dropForeignKey(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
} {
   <FOREIGN> <KEY> { s.add(this); } name = SimpleIdentifier()
   {
     return new SqlAlterDropForeign(s.end(this), name);
   }
}

SqlAlterTable dropPartition(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
} {
  <PARTITION> { s.add(this); } name = SimpleIdentifier()
  {
    return new SqlAlterDropPart(s.end(this), id, name);
  }
}

SqlAlterTable modifyColumn(Span s, String scope, SqlIdentifier id, SqlAlterTable alterTable): {
  DingoSqlColumn columnDec;
    final SqlDataTypeSpec type;
    ColumnOption columnOpt = null;
    SqlIdentifier afterCol = null;
    SqlIdentifier name = null;
} {
   [<COLUMN>] name = SimpleIdentifier()
    (
        type = DataType()
        columnOpt = parseColumnOption()
        [
          (
           <AFTER> afterCol = SimpleIdentifier()
           |
           <FIRST>
          )
        ]
        {
            boolean nullable = true;
            if (columnOpt != null) { nullable = columnOpt.nullable; }
            columnDec = DingoSqlDdlNodes.createColumn(s.add(id).end(this), name, type.withNullable(nullable), columnOpt);
        }
    )
   {
     if (alterTable == null) {
         return new SqlAlterModifyColumn(s.end(this), id, columnDec, afterCol);
     } else {
         SqlAlterModifyColumn alterModifyCol = (SqlAlterModifyColumn)alterTable;
         alterModifyCol.addSqlColumn(columnDec);
         alterModifyCol.addAlterColumn(afterCol);
         return alterModifyCol;
     }
   }
}

SqlAlterTable alterColumn(Span s, String scope, SqlIdentifier id): {
    SqlIdentifier name = null;
    SqlNode e = null;
    boolean visible = true;
}
{
    <COLUMN> { s.add(this); }
    name = SimpleIdentifier()
    (
      <SET>
      (
       <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY)
       |
       <VISIBLE> { visible = true;}
       |
       <INVISIBLE> { visible = false; }
      )
      {
        return new SqlAlterColumn(
            s.end(this), id, name, 1, e
        );
      }
    |
      <DROP> <DEFAULT_> { return new SqlAlterColumn(s.end(this), id, name, 2, e); }
    )
}

SqlAlterTable changeColumn(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
  SqlIdentifier newName = null;
  DingoSqlColumn columnDec = null;
    final SqlDataTypeSpec type;
   ColumnOption columnOpt = null;
} {
  [<COLUMN>] name = SimpleIdentifier() newName = SimpleIdentifier()
   (
        type = DataType()
        columnOpt = parseColumnOption()
        {
            boolean nullable = true;
            if (columnOpt != null) { nullable = columnOpt.nullable;}
            columnDec = DingoSqlDdlNodes.createColumn(s.add(id).end(this), name, type.withNullable(nullable), columnOpt);
        }
    )?
  {
    return new SqlAlterChangeColumn(s.end(this), id, name, newName, columnDec);
  }
}

SqlAlterTable alterAutoInc(Span s, String scope, SqlIdentifier id): {
    String auto = null;
}
{
    <EQ> { s.add(this); }
    <UNSIGNED_INTEGER_LITERAL> { auto = token.image; return new SqlAlterAutoIncrement(s.end(this), id, auto); }
}

SqlAlterTable alterRenameIndex(Span s, String scope, SqlIdentifier id): {
    String index = null;
    String toIndexName = null;
    String indexAlg = null;
    String indexLockOpt = null;
}
{
    <INDEX> { s.add(this); }
    { index = getNextToken().image; }
    <TO>
    { toIndexName = getNextToken().image; }
    (
     <COMMA>
     ( indexAlg = indexAlg()
     |
     indexLockOpt = indexLockOpt()
     )
    )*
    { return new SqlAlterRenameIndex(s.end(this), id, index, toIndexName);  }
}

SqlAlterTable alterTableComment(Span s, String scope, SqlIdentifier id): {
  String comment = null;
} {
   <EQ> { s.add(this); } (<IDENTIFIER>|<QUOTED_STRING>) { comment = token.image; }
   {
     return new SqlAlterTableComment(s.end(this), id, comment);
   }
}

SqlAlterTable alterTableTruncatePart(Span s, String scope, SqlIdentifier id): {
  SqlIdentifier name = null;
} {
  <PARTITION> { s.add(this); } name = SimpleIdentifier()
  {
    return new SqlAlterTruncatePart(s.end(this), id, name);
  }
}

SqlAlterTable alterExchange(Span s, String scope, SqlIdentifier id): {
 SqlIdentifier pName = null;
 SqlIdentifier withTableId = null;
} {
 <PARTITION> { s.add(this); } pName=SimpleIdentifier()
 <WITH> <TABLE> withTableId = CompoundIdentifier()
 {
   return new SqlAlterExchangePart(s.end(this), id, pName, withTableId);
 }
}
