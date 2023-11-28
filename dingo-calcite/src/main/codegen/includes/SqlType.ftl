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

SqlTypeNameSpec SqlTextTypeName(Span s): {
   final SqlTypeNameSpec sqlTypeNameSpec;
   int precision = -1;
   SqlTypeName typeName;
} {
   <TEXT>
    {
        s.add(this);
        typeName = SqlTypeName.VARCHAR;
        precision = 65535;
        return new SqlBasicTypeNameSpec(typeName, precision, s.end(this));
    }    
}

SqlTypeNameSpec SqlLongTextTypeName(Span s): {
   final SqlTypeNameSpec sqlTypeNameSpec;
   int precision = -1;
   SqlTypeName typeName;
} {
   <LONGTEXT>
    {
        s.add(this);
        typeName = SqlTypeName.VARCHAR;
        precision = Integer.MAX_VALUE;
        return new SqlBasicTypeNameSpec(typeName, precision, s.end(this));
    }    
}

SqlTypeNameSpec SqlDateTimeTypeName(Span s): {
   final SqlTypeNameSpec sqlTypeNameSpec;
   int precision = -1;
    SqlTypeName typeName;
    boolean withLocalTimeZone = false;
} {
   <DATETIME>
   { s.add(this); }
    precision = PrecisionOpt()
    withLocalTimeZone = TimeZoneOpt()
    {
        if (withLocalTimeZone) {
            typeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            typeName = SqlTypeName.TIMESTAMP;
        }
        return new SqlBasicTypeNameSpec(typeName, precision, s.end(this));
    }    
}

SqlTypeNameSpec SqlFloatTypeName(Span s) :
{
    final SqlTypeNameSpec sqlTypeNameSpec;
}
{
    <FLOAT>
    {
        s.add(this);
        SqlTypeName sqlTypeName = SqlTypeName.FLOAT;
        int precision = -1;
    }
        [
            <LPAREN>
                precision = UnsignedIntLiteral()
            <RPAREN>
        ]
    {
        sqlTypeNameSpec = new SqlFloatTypeNameSpec(sqlTypeName, precision, s.end(this));
        return sqlTypeNameSpec;
    }
}
