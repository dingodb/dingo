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


SqlGrant SqlGrant() : {
 final Span s;
 final SqlIdentifier subject;
 boolean isAllPrivileges = false;
 SqlIdentifier userIdentifier;
 String user;
 String host = "%";
 String privilege = "";
 boolean withGrantOption = false;
 List<String> privilegeList = new ArrayList();
} {
   <GRANT> { s = span(); }
   [ <ALL> <PRIVILEGES> { isAllPrivileges = true; } ]
   [
     privilege = privilege() { privilegeList.add(privilege.toLowerCase()); }
     (
       <COMMA> privilege = privilege() { privilegeList.add(privilege.toLowerCase()); }
     )*
   ]
   <ON>
   subject = getSchemaTable()
   <TO>
   ( <QUOTED_STRING> | <IDENTIFIER> )
   { user = token.image; }
    [<AT_SPLIT> (<QUOTED_STRING>|<IDENTIFIER>) { host = token.image;} ]
    [ <WITH> <GRANT> <OPTION> { withGrantOption = true; } ]
    {
        return new SqlGrant(s.end(this), isAllPrivileges, privilegeList, subject, user, host, withGrantOption);
    }
}

SqlRevoke SqlRevoke() : {
 final Span s;
 SqlIdentifier subject = null;
 boolean isAllPrivileges = false;
 String user = "";
 String host = "%";
 String privilege = "";
 List<String> privilegeList = new ArrayList();
} {
   <REVOKE> { s = span(); }
   [ <ALL> <PRIVILEGES> { isAllPrivileges = true; } ]
   [
     privilege = privilege() { privilegeList.add(privilege); }
     (
       <COMMA> privilege = privilege()
       { privilegeList.add(privilege); }
     )*
   ]
   <ON>
   subject = getSchemaTable()
   <FROM>
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { user = user = token.image; }
    [<AT_SPLIT> (<QUOTED_STRING> | <IDENTIFIER>) {host = token.image; } ]
    {
        return new SqlRevoke(s.end(this), isAllPrivileges, privilegeList, subject, user, host);
    }
}

SqlFlushPrivileges SqlFlush ():{
  final Span s;
} {
   <FLUSH> { s = span(); } <PRIVILEGES> { return new SqlFlushPrivileges(s.end(this)); }
}

String privilege() : {
   String privilege = "";
}
{
  ( <SELECT>
  | <UPDATE>
  | <INSERT>
  | <DELETE>
  | <DROP>
  | <GRANT>
  | <REVOKE>
  | <INDEX>
  | <ALTER>
  | <RELOAD>
  )
  {
     return token.image.toLowerCase();
  }
  |
    <CREATE>
    [ <VIEW> { return "create view"; }]
    [ <USER> { return "create user"; }]
    { return token.image; }
  |
    <SHOW><DATABASES>
    { return "show databases"; }
}