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


SqlShow SqlShow(): {
  final Span s;
  final SqlShow show;
} {
  <SHOW> { s = span(); }
  (
    show = SqlShowGrants(s)
    |
    show = SqlShowWarnings(s)
    |
    show = SqlShowDatabases(s)
    |
    show = SqlShowTables(s)
    |
    show = SqlShowFullTables(s)
    |
    show = SqlShowVariables(s)
    |
    show = SqlShowGlobalVariables(s)
    |
    show = SqlShowCreate(s)
    |
    show = SqlShowColumns(s)
    |
    show = SqlShowTable(s)
    |
    show = SqlShowLocks(s)
    |
    show = SqlShowEngines(s)
    |
    show = SqlShowCollation(s)
    |
    show = SqlShowCharset(s)
    |
    show = SqlShowPlugins(s)
    |
    show = SqlShowProcess(s)
  )
  {
    return show;
  }
}

SqlShow SqlShowTable(Span s): {
   SqlIdentifier tableName = null;
   String schema = null;
   String pattern = null;
} {
   <TABLE>
   (
    <STATUS> [ <FROM> (<QUOTED_STRING> | <IDENTIFIER>) { schema = token.image.toUpperCase(); } ]
     [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
     {
       return new SqlShowTableStatus(s.end(this), schema, pattern);
     }
    |
     tableName = CompoundTableIdentifier()
     <DISTRIBUTION> { return new SqlShowTableDistribution(s.end(this), tableName); }
   )
}

SqlShow SqlShowColumns(Span s): {
   SqlIdentifier tableName = null;
   String pattern = null;
} {
   <COLUMNS> <FROM> tableName = CompoundTableIdentifier()
   [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
   { return new SqlShowColumns(s.end(this), tableName, pattern); }
}

SqlShow SqlShowCreate(Span s): {
   SqlIdentifier tableName = null;
   String userName = null;
   String host = "%";
} {
   <CREATE>
   (
       <TABLE> tableName = CompoundTableIdentifier()
       { return new SqlShowCreateTable(s.end(this), tableName); }
   |
       <USER>
       <QUOTED_STRING> { userName = token.image.replace("'", ""); }
       [<AT_SPLIT> <QUOTED_STRING> { host = token.image.replace("'", "");} ]
       { return new SqlShowCreateUser(s.end(this), userName, host); }
   )
}

SqlShow SqlShowDatabases(Span s): {
   String pattern = null;
} {
  <DATABASES> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowDatabases(s.end(this), pattern); }
}

SqlShow SqlShowTables(Span s): {
   String pattern = null;
} {
  <TABLES> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowTables(s.end(this), pattern); }
}

SqlShow SqlShowFullTables(Span s): {
   String schema = null;
   String pattern = null;
   SqlNode condition = null;
} {
  <FULL>
  <TABLES> [ <FROM> (<BACK_QUOTED_IDENTIFIER> { schema = token.image; } | <IDENTIFIER> { schema = token.image; })]
  [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); }  ]
  [ condition = Where() ]
  { return new SqlShowFullTables(s.end(this), schema, pattern, condition); }
}

SqlShow SqlShowWarnings(Span s): {

} {
  <WARNINGS>
  {
    return new SqlShowWarnings(s.end(this));
  }
}

SqlShow SqlShowGrants(Span s): {
  SqlIdentifier userIdentifier;
  String user = null;
  String host = "%";
} {
  <GRANTS> <FOR>
  [
       <QUOTED_STRING> { user = token.image; }
       [<AT_SPLIT> <QUOTED_STRING> { host = token.image;} ]
       {
       return new SqlShowGrants(s.end(this), user, host);
       }
  ]
  userIdentifier = CompoundIdentifier() { user = userIdentifier.getSimple(); }
  [<AT_SPLIT> (<QUOTED_STRING> | <IDENTIFIER>) {host = token.image; } ]
  {
    return new SqlShowGrants(s.end(this), user, host);
  }
}

SqlShow SqlShowVariables(Span s): {
  String pattern = null;
  boolean isGlobal = false;
} {
  <VARIABLES> [ <LIKE> <QUOTED_STRING> { pattern = token.image.replace("'", ""); } ]
  {
    return new SqlShowVariables(s.end(this), pattern, isGlobal);
  }
}

SqlShow SqlShowGlobalVariables(Span s): {
  String pattern = null;
  boolean isGlobal = true;
} {
  <GLOBAL> <VARIABLES> [ <LIKE> <QUOTED_STRING> { pattern = token.image.replace("'", ""); } ]
  {
    return new SqlShowVariables(s.end(this), pattern, isGlobal);
  }
}

SqlShow SqlShowLocks(Span s): {
  SqlNode condition = null;
  SqlIdentifier id = null;
  SqlKind kind = null;
  Object operand = null;
}{
  <LOCKS>
// todo support more filter
  [ <WHERE> id = SimpleIdentifier() kind = comp() operand = anything() ]
  {return new SqlShowLocks(s.end(this), id, kind, operand); }
}

SqlShow SqlShowEngines(Span s): {
  String pattern = null;
} {
  <ENGINES> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowEngines(s.end(this), pattern); }
}

SqlShow SqlShowCollation(Span s): {
  String pattern = null;
} {
  <COLLATION> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowCollation(s.end(this), pattern); }
}

SqlShow SqlShowCharset(Span s): {
  String pattern = null;
} {
  <CHARSET> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowCharset(s.end(this), pattern); }
}

SqlShow SqlShowPlugins(Span s): {
  String pattern = null;
} {
  <PLUGINS> [ <LIKE> <QUOTED_STRING> { pattern = token.image.toUpperCase().replace("'", ""); } ]
  { return new SqlShowPlugins(s.end(this), pattern); }
}

SqlShow SqlShowProcess(Span s): {
  String pattern = null;
} {
  <PROCESSLIST>
  { return new SqlShowProcessList(s.end(this)); }
}
