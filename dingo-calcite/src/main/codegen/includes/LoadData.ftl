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
  List<LoadDataColMapping> withColumnList = null;
  LoadDataSetExpr loadDataSetExpr = null;
  boolean replaceInto = false;
} {
  <LOAD> { s = span(); }
  <DATA> [<CONCURRENT>][<LOCAL> { local = true; }] <INFILE>
  <QUOTED_STRING> { filePath = token.image.replace("'", "").toLowerCase(); }
  [ <IGNORE> {ignore = true;}]
  [ <REPLACE> { replaceInto = true; }]
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
  [ loadDataSetExpr = setLoadProp()]
  { return new SqlLoadData(s.end(this), table, filePath, terminated, escaped, lineTerminated, enclosed, lineStarting, exportCharset, ignoreNum, local, ignore, withColumnList, loadDataSetExpr, replaceInto); }
}

List<LoadDataColMapping> loadDataParenthesizedSimpleIdentifierList() :
{
    final Span s;
    final List<LoadDataColMapping> list = new ArrayList<>();
}
{
    <LPAREN> { s = span(); }
    AddLoadDataSimpleIdentifiers(list)
    <RPAREN> {
        return list;
    }
}

void AddLoadDataSimpleIdentifiers(List<LoadDataColMapping> list) :
{
    SqlIdentifier id;
    boolean userVar = false;
}
{
    [<AT_SPLIT> {userVar = true;}]
    id = SimpleIdentifier() {list.add(new LoadDataColMapping(id, userVar));}
    (
        <COMMA> {userVar = false;} [<AT_SPLIT> { userVar = true; }]id = SimpleIdentifier() {
            list.add(new LoadDataColMapping(id,userVar));
        } [<ASC>] [<DESC>]
    )*
}

LoadDataSetExpr setLoadProp() :
{
    final Span s;
    SqlNodeList targetColumnList;
    SqlNodeList sourceExpressionList;
    SqlIdentifier id;
}
{
    <SET> { s = span(); targetColumnList = new SqlNodeList(s.pos()); sourceExpressionList = new SqlNodeList(s.pos()); }
    id = SimpleIdentifier()
    {
       targetColumnList.add(id);
    }
    <EQ>
    AddExpression(sourceExpressionList, ExprContext.ACCEPT_SUB_QUERY)
    (
     <COMMA>
     id = SimpleIdentifier() { targetColumnList.add(id); }
     <EQ>
     AddExpression(sourceExpressionList, ExprContext.ACCEPT_SUB_QUERY)
    )*
    {
        return new LoadDataSetExpr(targetColumnList, sourceExpressionList);
    }
}

