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

SqlAlterTable SqlAlterTable(Span s, String scope): {
    SqlIdentifier id;
    SqlAlterTable alterTable;
} {
    <TABLE> id = CompoundIdentifier()
    <ADD>
    (
        alterTable = addPartition(s, scope, id)
        |
        alterTable = addIndex(s, scope, id)
        |
        alterTable = addColumn(s, scope, id)
    )
    { return alterTable; }
}

SqlAlterTable addPartition(Span s, String scope, SqlIdentifier id): {
} {
    <DISTRIBUTION> <BY>
    {
        return new SqlAlterTableDistribution(s.end(this), id, readPartitionDetails().get(0));
    }
}

SqlAlterTable addColumn(Span s, String scope, SqlIdentifier id): {
    final SqlIdentifier columnId;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    final ColumnStrategy strategy;
} {
    <COLUMN>
    columnId = SimpleIdentifier()
    type = DataType()
    nullable = NullableOptDefaultTrue()
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
        <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY)
        { strategy = ColumnStrategy.DEFAULT; }
    |
        {
            e = null;
            strategy = nullable ? ColumnStrategy.NULLABLE: ColumnStrategy.NOT_NULLABLE;
        }
    )
    {
        return new SqlAlterAddColumn(s.end(this), id, DingoSqlDdlNodes.createColumn(
            s.end(this), columnId, type.withNullable(nullable), e, strategy, false
        ));
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
    final SqlNodeList columnList;
} {
    <INDEX> { s.add(this); }
    { index = getNextToken().image; }
    (
        <VECTOR> { indexType = "vector"; } columnList = ParenthesizedSimpleIdentifierList()
    |
        [<SCALAR>] columnList = ParenthesizedSimpleIdentifierList()
    )
    [ <WITH> withColumnList = ParenthesizedSimpleIdentifierList() ]
    [
        <PARTITION> <BY>
           {
               partitionDefinition = new PartitionDefinition();
               partitionDefinition.setFuncName(getNextToken().image);
               partitionDefinition.setCols(readNames());
               partitionDefinition.setDetails(readPartitionDetails());
           }
    ]
    [
        <REPLICA> <EQ> {replica = Integer.parseInt(getNextToken().image);}
    ]
    [ <PARAMETERS> properties = readProperties() ]
    {
        return new SqlAlterAddIndex(
            s.end(this), id,
            new SqlIndexDeclaration(
                s.end(this), index, columnList, withColumnList, properties,partitionDefinition, replica, indexType
            )
        );
    }
}

//SqlAlterTable dropColumn(Span s, String scope, SqlIdentifier id): {
//}
//{
//    { retrun null; }
//}
