/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite;

import io.dingodb.calcite.type.DingoSqlTypeFactory;
import it.unimi.dsi.fastutil.Hash;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.HashMap;
import java.util.Map;

public class DingoTypeMapper {
    /**
     * Type mapper to compatible with mysql in union and some other scenarios.
     */
    public static final Map<SqlTypeName, Map<SqlTypeName, SqlTypeName>> MYSQL_TYPE_MAP = new HashMap<>() {{
       put(SqlTypeName.DOUBLE, new HashMap<>() {{
           put(SqlTypeName.FLOAT, SqlTypeName.DOUBLE);
           put(SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
           put(SqlTypeName.INTEGER, SqlTypeName.DOUBLE);
           put(SqlTypeName.BIGINT, SqlTypeName.DOUBLE);
           put(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
       }});
       put(SqlTypeName.FLOAT, new HashMap<>() {{
            put(SqlTypeName.FLOAT, SqlTypeName.FLOAT);
            put(SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
            put(SqlTypeName.INTEGER, SqlTypeName.DOUBLE);
           put(SqlTypeName.BIGINT, SqlTypeName.FLOAT);
           put(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
        }});
       put(SqlTypeName.INTEGER, new HashMap<>() {{
            put(SqlTypeName.FLOAT, SqlTypeName.DOUBLE);
            put(SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
            put(SqlTypeName.INTEGER, SqlTypeName.INTEGER);
           put(SqlTypeName.BIGINT, SqlTypeName.BIGINT);
           put(SqlTypeName.BOOLEAN, SqlTypeName.INTEGER);
        }});
       put(SqlTypeName.BIGINT, new HashMap<>() {{
            put(SqlTypeName.FLOAT, SqlTypeName.DOUBLE);
            put(SqlTypeName.DOUBLE, SqlTypeName.DOUBLE);
            put(SqlTypeName.INTEGER, SqlTypeName.BIGINT);
            put(SqlTypeName.BIGINT, SqlTypeName.BIGINT);
            put(SqlTypeName.BOOLEAN, SqlTypeName.BIGINT);
            put(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
        }});
       put(SqlTypeName.BOOLEAN, new HashMap<>() {{
            put(SqlTypeName.BIGINT, SqlTypeName.BIGINT);
            put(SqlTypeName.INTEGER, SqlTypeName.INTEGER);
        }});
       put(SqlTypeName.VARCHAR, new HashMap<>() {{
            put(SqlTypeName.FLOAT, SqlTypeName.VARCHAR);
            put(SqlTypeName.DOUBLE, SqlTypeName.VARCHAR);
            put(SqlTypeName.BIGINT, SqlTypeName.VARCHAR);
        }});
    }};

    /**
     * Get preferred type name.
     * @param left  left type.
     * @param right right type.
     * @return  preferred type.
     */
    public static SqlTypeName getPreferredType(SqlTypeName left, SqlTypeName right) {
        //Get left mapper.
        return MYSQL_TYPE_MAP.containsKey(left) ? MYSQL_TYPE_MAP.get(left).get(right) : left;
    }

    /**
     * Get preferred type name.
     * @param left  left type.
     * @param right right type.
     * @return  preferred type.
     */
    public static RelDataType getLeastRestrictPreferredType(RelDataType left, RelDataType right, DingoSqlTypeFactory typeFactory) {
        SqlTypeName leftName = left.getSqlTypeName();
        SqlTypeName rightName = right.getSqlTypeName();

        SqlTypeName preferredType = getPreferredType(leftName, rightName);
        if(preferredType == null) {
            return left;
        }

        if (preferredType == leftName) {
            return left;
        } else if(preferredType == rightName) {
            return right;
        } else {
            return typeFactory.createSqlType(preferredType);
        }
    }

    /*
        To be compatible with MYSQL binary arithmetic operator.
        The relation between parameter type and result type are as following:
        +,-,*,/,% operators:
                ------------------------------------------------------------------------------------
                |    left parameter type      |    right parameter type    |    result type        |
                ------------------------------------------------------------------------------------
                |          float              |        double              |        double         |
                ------------------------------------------------------------------------------------
                |          float              |        float               |        float          |
                ------------------------------------------------------------------------------------
                |          float              |        int                 |        double         |
                ------------------------------------------------------------------------------------
                |          float              |        bigint              |        double         |
                ------------------------------------------------------------------------------------
                |          double             |        float               |        double         |
                ------------------------------------------------------------------------------------
                |          double             |        double              |        double         |
                ------------------------------------------------------------------------------------
                |          double             |        int                 |        double         |
                ------------------------------------------------------------------------------------
                |          double             |        bigint              |        double         |
                ------------------------------------------------------------------------------------
                |          int                |        bigint              |        bigint         |
                ------------------------------------------------------------------------------------
                |          int                |        float               |        double         |
                ------------------------------------------------------------------------------------
                |          int                |        double              |        double         |
                ------------------------------------------------------------------------------------
                |          bigint             |        bigint              |        bigint         |
                ------------------------------------------------------------------------------------
                |          bigint             |        int                 |        double         |
                ------------------------------------------------------------------------------------
                |          bigint             |        float               |        double         |
                ------------------------------------------------------------------------------------
     */
    public static RelDataType getBinaryArithmeticResultType(RelDataType left, RelDataType right, RelDataTypeFactory factory) {
        RelDataType ret = null;

        if (SqlTypeUtil.isFloat(left)) {
            if (SqlTypeUtil.isDouble(right)) {
                ret = right;
            } else if(SqlTypeUtil.isFloat(right)) {
                ret = left;
            } else if(SqlTypeUtil.isInt(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            } else if(SqlTypeUtil.isBigint(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            }
        } else if(SqlTypeUtil.isDouble(left)) {
            if (SqlTypeUtil.isFloat(right)) {
                ret = left;
            } else if(SqlTypeUtil.isDouble(right)) {
                ret = left;
            } else if(SqlTypeUtil.isInt(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            } else if(SqlTypeUtil.isBigint(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            }
        } else if(SqlTypeUtil.isInt(left)) {
            if (SqlTypeUtil.isBigint(right)) {
                ret = right;
            } else if(SqlTypeUtil.isFloat(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            } else if(SqlTypeUtil.isDouble(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            }
        } else if(SqlTypeUtil.isBigint(left)) {
            if(SqlTypeUtil.isInt(right)) {
                ret = left;
            } else if(SqlTypeUtil.isFloat(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            } else if(SqlTypeUtil.isDouble(right)) {
                ret = SqlTypeUtil.getDouble(factory);
            }
        } else if(SqlTypeUtil.isCharacter(left)) {  //char or varchar.
            if(SqlTypeUtil.isCharacter(right)) {    //char or varchar.
                ret = SqlTypeUtil.getDouble(factory);
            }
        }

        return ret;
    }


    /*
        To be compatible with MYSQL aggregation.
        The relation between parameter type and result type are as following:
            SUM:
                -------------------------------------------------
                |    parameter type      |   result type        |
                -------------------------------------------------
                |    varchar             |   double             |
                |    char                |   double             |
                |    tinyint             |   decimal            |
                |    smallint            |   decimal            |
                |    mediumint           |   decimal            |
                |    int                 |   decimal            |
                |    bigint              |   decimal            |
                -------------------------------------------------
            AVG:
                -------------------------------------------------
                |    parameter type      |   result type        |
                -------------------------------------------------
                |    varchar             |   double             |
                |    char                |   double             |
                -------------------------------------------------
     */
    public static RelDataType getAggregateResultType(SqlAggFunction func, RelDataType sourceType) {
        //common for SUM, AVG
        if(func instanceof SqlSumAggFunction || func instanceof SqlAvgAggFunction) {
            if (sourceType.getSqlTypeName() == SqlTypeName.VARCHAR) {
                return DingoSqlTypeFactory.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
            } else if(sourceType.getSqlTypeName() == SqlTypeName.CHAR) {
                return DingoSqlTypeFactory.INSTANCE.createSqlType(SqlTypeName.DOUBLE);
            }
        }

        //If no rule for mapper then we return the origin type.
        return null;
    }
}
