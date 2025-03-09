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


SqlLoadData SqlLoadData(): {
  final Span s;
  String filePath = null;
  final SqlIdentifier table;
  byte[] terminated = "	".getBytes();
  String enclosed = null;
  byte[] escaped = "\\".getBytes();
  byte[] lineTerminated = new byte[]{0x0a};
  byte[] lineStarting = null;
  String exportCharset = null;
  int ignoreNum = 0;
  boolean local = false;
  boolean ignore = false;
  SqlNodeList withColumnList = null;
  SqlNodeList setColumnList = null;
} {
  <LOAD> { s = span(); }
  <DATA> [<CONCURRENT>][<LOCAL> { local = true; }] <INFILE>
  <QUOTED_STRING> { filePath = token.image.replace("'", "").toLowerCase(); }
  [ <IGNORE> {ignore = true;}]
  <INTO> <TABLE> table = CompoundIdentifier()
   (
      <CHARACTER> <SET>  { exportCharset = dingoIdentifier(); }
   |
     <FIELDS>
     (<TERMINATED> <BY> [<QUOTED_STRING> { terminated = getSpecialBytes(token.image); }]
                        [<BINARY_STRING_LITERAL> { terminated = getSpecialHexBytes(token.image);}]
     |
      <ENCLOSED> <BY> <QUOTED_STRING> { enclosed = getEnclosed(token.image); }
     |
      <ESCAPED> <BY> [<QUOTED_STRING> { escaped = getSpecialBytes(token.image); }]
                     [<BINARY_STRING_LITERAL> { escaped = getSpecialHexBytes(token.image);}]
     )*
   |
     <LINES>
     ( <STARTING> <BY> [<QUOTED_STRING> { lineStarting = getSpecialBytes(token.image); }]
                       [<BINARY_STRING_LITERAL> { lineStarting = getSpecialHexBytes(token.image); }]
     |
      <TERMINATED> <BY> [<QUOTED_STRING> { lineTerminated = getSpecialBytes(token.image); } ]
                         [<BINARY_STRING_LITERAL> { lineTerminated = getSpecialHexBytes(token.image); }]
     )*
   |
     <IGNORE> (<UNSIGNED_INTEGER_LITERAL> | <DECIMAL_NUMERIC_LITERAL>) { ignoreNum = Integer.parseInt(token.image); }
   )*
  [ withColumnList = loadDataParenthesizedSimpleIdentifierList()]
  [ setColumnList = setLoadProp()]
  { return new SqlLoadData(s.end(this), table, filePath, terminated, escaped, lineTerminated, enclosed, lineStarting, exportCharset, ignoreNum, local, ignore, withColumnList, setColumnList); }
}

SqlNodeList loadDataParenthesizedSimpleIdentifierList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AddLoadDataSimpleIdentifiers(list)
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AddLoadDataSimpleIdentifiers(List<SqlNode> list) :
{
    SqlIdentifier id;
}
{
    [<AT_SPLIT>]
    id = SimpleIdentifier() {list.add(id);}
    (
        <COMMA> [<AT_SPLIT>]id = SimpleIdentifier() {
            list.add(id);
        } [<ASC>] [<DESC>]
    )*
}

SqlNodeList setLoadProp() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlIdentifier id;
    SqlIdentifier tid = null;
    SqlIdentifier hexId = null;
}
{
    <SET> { s = span(); }
    id = SimpleIdentifier()
    <EQ>
    (
      <AT_SPLIT> tid = SimpleIdentifier()
     |
      <UNHEX> <LPAREN> <AT_SPLIT>hexId = SimpleIdentifier() <RPAREN>
    )
    {
     if (hexId != null) {
      list.add(hexId);
     }
    }
    (
     <COMMA>
     id = SimpleIdentifier()
     <EQ>
     (
       <AT_SPLIT> tid = SimpleIdentifier()
      |
       <UNHEX> <LPAREN> <AT_SPLIT> hexId = SimpleIdentifier() <RPAREN>
     )
     { 
       if (hexId != null) {
        list.add(hexId);
       }
     }
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

