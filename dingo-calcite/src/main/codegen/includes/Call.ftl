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


SqlCall SqlCall(): {
  final Span s;
  final SqlIdentifier call;
  Object[] vals;
} {
   <CALL> { s = span(); }
   call = CompoundTableIdentifier()
   <LPAREN>
   vals = readValues()
   <RPAREN>
   {
     return new io.dingodb.calcite.grammar.ddl.SqlCall(s.end(this), call, vals);
   }
}
