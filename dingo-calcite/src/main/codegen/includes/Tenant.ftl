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

SqlCreate SqlCreateTenant(Span s, boolean replace) :
{
    final boolean ifNotExists;
    String name;
    String remarks = null;
}
{
    <TENANT> ifNotExists = IfNotExistsOpt()
    name = dingoIdentifier()
    (
     <REMARKS> <EQ> { remarks = getNextToken().image; }
    )*
    {
        return new SqlCreateTenant(s.end(this), replace, ifNotExists, name, remarks);
    }
}

SqlDrop SqlDropTenant(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    String name;
}
{
    <TENANT> ifExists = IfExistsOpt()
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { name = token.image; }
    {
        return new SqlDropTenant(s.end(this), ifExists, name);
    }
}

SqlAlterTenant SqlAlterTenant(Span s, String scope) :
{
    final String oldName;
    String newName;
}
{
    <TENANT>
    ( <QUOTED_STRING> | <IDENTIFIER> )
    { s = span(); oldName = token.image; }
    <RENAME> <AS> (<QUOTED_STRING> | <IDENTIFIER>) { newName = token.image; }
    {
        return new SqlAlterTenant(oldName, newName, s.end(this));
    }
}
