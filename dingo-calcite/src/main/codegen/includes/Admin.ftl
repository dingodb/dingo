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


SqlAdmin SqlAdmin(): {
  BigInteger txnId;
  BigInteger point;
  String timeStr = null;
  final Span s;
} {
  <ADMIN> { s = span(); }
  (
  <ROLLBACK>
  <UNSIGNED_INTEGER_LITERAL>
  { txnId = new BigInteger(token.image); }
  { return new SqlAdminRollback(s.end(this), txnId); }
  |
   <RESET> <AUTO_INCREMENT> { return new SqlAdminResetAutoInc(s.end(this)); }
  |
   <BACK_UP_TSO_POINT> { point = new BigInteger(getNextToken().image); }
   { return new SqlBackUpTsoPoint(s.end(this), point); }
  |
   <BACK_UP_TIME_POINT> { timeStr = getNextToken().image.toUpperCase().replace("'", ""); }
   { return new SqlBackUpTimePoint(s.end(this), timeStr); }
  |
   <START_GC>
   { return new SqlStartGc(s.end(this)); }
  )
}
