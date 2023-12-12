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
    (
	    <ADD>
	    (
	        alterTable = addPartition(s, scope, id)
	    |
	        alterTable = addIndex(s, scope, id)
	    )
	  |
	    <CONVERT> <TO>
	    alterTable = convertCharset(s, id)
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

SqlAlterTable addIndex(Span s, String scope, SqlIdentifier id): {
    final String index;
    SqlIdentifier column;
    List<SqlIdentifier> columns;
    boolean isUnique = false;
} {
    [ <INDEX> ] [<UNIQUE> { isUnique = true;} ]
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { index = token.image.toUpperCase(); }
    <LPAREN>
        column = SimpleIdentifier() { columns = new ArrayList<SqlIdentifier>(); columns.add(column); }
        (
            <COMMA> column = SimpleIdentifier() { columns.add(column); }
        )*
    <RPAREN>
    { return new SqlAlterAddIndex(s.end(this), id, index, columns, isUnique); }
}

SqlAlterTable convertCharset(Span s, SqlIdentifier id): {
    final String charset;
    String collate = "utf8_bin";
} {
  <CHARACTER> <SET> { charset = this.getNextToken().image; } [<COLLATE> { collate = this.getNextToken().image; }]
  { return new SqlAlterConvertCharset(s.end(this), id, charset, collate); }
}