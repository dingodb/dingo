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


SqlBeginTx SqlStartTx(): {
  final Span s;
  String txnMode = "";
} {
  <START> { s = span(); } <TRANSACTION> [<PESSIMISTIC> { txnMode = "PESSIMISTIC"; } | <OPTIMISTIC> { txnMode = "OPTIMISTIC"; }]{ return new SqlBeginTx(s.end(this), txnMode); }
}

SqlBeginTx SqlBegin(): {
  final Span s; String txnMode = "";
} {
  <BEGIN> [<PESSIMISTIC> { txnMode = "PESSIMISTIC"; } | <OPTIMISTIC> { txnMode = "OPTIMISTIC"; }] { s = span();  return new SqlBeginTx(s.end(this), txnMode); }
}

SqlLock SqlLock(): {
  final Span s;
  List<SqlIdentifier> tableNameList = new ArrayList<>();
  List<SqlBlock> sqlBlockList = new ArrayList<>();
} {
  <LOCK> { s = span(); }
  (<TABLES> { tableNameList.add(CompoundIdentifier()); }
   (
     <COMMA>
     { tableNameList.add(CompoundIdentifier()); }
   )*
   { return new SqlLockTable(s.end(this), tableNameList); }
  |
   <BLOCKS>
   {
     sqlBlockList.add(readBlock());
   }
   (
     <COMMA>
     { sqlBlockList.add(readBlock()); }
   )*
   { return new SqlLockBlock(s.end(this), sqlBlockList); }
  )
}

SqlBlock readBlock() : {
   String table;
   Number hash = 0;
   String funcName = "RANGE";
   List<Object> values = new ArrayList<Object>();
}{
    { table = getNextToken().image; }
    [ <HASH> <LPAREN> { hash = number(); funcName = "HASH"; } <RPAREN> ]
    <RANGE>
    <LPAREN>
    { values.add(anything());}
    (
      <COMMA>
      { values.add(anything());}
    )*
    <RPAREN>
    <AS>
     { return new SqlBlock(getNextToken().image, values.toArray(), table, funcName, hash.intValue()); }
}

SqlEnd SqlEnd(): {
  final Span s;
  boolean pessimistic = false;
} {
  <END> { s = span(); } [ <PESSIMISTIC> {pessimistic = true;}]
  {return new SqlEnd(s.end(this), pessimistic); }
}

SqlUnLock SqlUnLock(): {
  final Span s;
} {
  <UNLOCK> { s = span(); }
  (<TABLES>
   { return new SqlUnLockTable(s.end(this)); }
  |
   <BLOCKS>
   { return new SqlUnLockBlock(s.end(this)); }
  )
}
