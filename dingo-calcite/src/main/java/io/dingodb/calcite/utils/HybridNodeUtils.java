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

package io.dingodb.calcite.utils;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql2rel.SqlHybridSearchOperator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HybridNodeUtils {

    public static void lockUpHybridSearchNode(SqlNode sqlNode, SqlNode subSqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlNode from = ((SqlSelect) sqlNode).getFrom();
            if (from instanceof SqlBasicCall && (((SqlBasicCall) from).getOperator() instanceof SqlHybridSearchOperator)) {
                ((SqlSelect) sqlNode).setFrom(subSqlNode);
            } else {
                if (from instanceof SqlJoin) {
                    lockUpHybridSearchNode(((SqlJoin) from).getLeft(), subSqlNode);
                    lockUpHybridSearchNode(((SqlJoin) from).getRight(), subSqlNode);
                }
            }
            lockUpHybridSearchNode(((SqlSelect) sqlNode).getWhere(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getGroup(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getOrderList(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getSelectList(), subSqlNode);
        } else if (sqlNode instanceof SqlBasicCall) {
            if (((SqlBasicCall) sqlNode).getOperator() instanceof SqlAsOperator) {
                deepLockUpChildren(((SqlBasicCall) sqlNode).getOperandList(), subSqlNode);
            } else {
                deepLockUpChildren(((SqlBasicCall) sqlNode).getOperandList(), subSqlNode);
            }
        } else if (sqlNode instanceof SqlOrderBy) {
            lockUpHybridSearchNode(((SqlOrderBy)sqlNode).query, subSqlNode);
        } else if (sqlNode instanceof SqlExplain) {
            lockUpHybridSearchNode(((SqlExplain) sqlNode).getExplicandum(), subSqlNode);
        } else if (sqlNode instanceof SqlInsert) {
            lockUpHybridSearchNode(((SqlInsert)sqlNode).getSource(), subSqlNode);
        } else if (sqlNode instanceof SqlUpdate) {
            lockUpHybridSearchNode(((SqlUpdate)sqlNode).getCondition(), subSqlNode);
        } else if (sqlNode instanceof SqlDelete) {
            lockUpHybridSearchNode(((SqlDelete)sqlNode).getCondition(), subSqlNode);
        }
    }

    public static void lockUpHybridSearchNode(SqlNode sqlNode, ConcurrentHashMap<SqlBasicCall, SqlNode> subSqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlNode from = ((SqlSelect) sqlNode).getFrom();
            if (from instanceof SqlBasicCall && (((SqlBasicCall) from).getOperator() instanceof SqlHybridSearchOperator)) {
                SqlBasicCall removeKey = null;
                for (Map.Entry<SqlBasicCall, SqlNode> entry : subSqlNode.entrySet()) {
                    SqlBasicCall key = entry.getKey();
                    SqlNode value = entry.getValue();
                    if (from.toString().equals(key.toString())) {
                        ((SqlSelect) sqlNode).setFrom(value);
                        removeKey = key;
                        break;
                    }
                }
                if (removeKey != null) {
                    subSqlNode.remove(removeKey);
                }
            } else {
                if (from instanceof SqlJoin) {
                    lockUpHybridSearchNode(((SqlJoin) from).getLeft(), subSqlNode);
                    lockUpHybridSearchNode(((SqlJoin) from).getRight(), subSqlNode);
                }
            }
            lockUpHybridSearchNode(((SqlSelect) sqlNode).getWhere(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getGroup(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getOrderList(), subSqlNode);
            deepLockUpChildren(((SqlSelect) sqlNode).getSelectList(), subSqlNode);
        } else if (sqlNode instanceof SqlJoin) {
            lockUpHybridSearchNode(((SqlJoin) sqlNode).getLeft(), subSqlNode);
            lockUpHybridSearchNode(((SqlJoin) sqlNode).getRight(), subSqlNode);
        } else if (sqlNode instanceof SqlBasicCall) {
            if (((SqlBasicCall) sqlNode).getOperator() instanceof SqlAsOperator) {
                deepLockUpChildren(((SqlBasicCall) sqlNode).getOperandList(), subSqlNode);
            } else {
                deepLockUpChildren(((SqlBasicCall) sqlNode).getOperandList(), subSqlNode);
            }
        } else if (sqlNode instanceof SqlOrderBy) {
            lockUpHybridSearchNode(((SqlOrderBy)sqlNode).query, subSqlNode);
        } else if (sqlNode instanceof SqlExplain) {
            lockUpHybridSearchNode(((SqlExplain) sqlNode).getExplicandum(), subSqlNode);
        } else if (sqlNode instanceof SqlInsert) {
            lockUpHybridSearchNode(((SqlInsert)sqlNode).getSource(), subSqlNode);
        } else if (sqlNode instanceof SqlUpdate) {
            lockUpHybridSearchNode(((SqlUpdate)sqlNode).getCondition(), subSqlNode);
        } else if (sqlNode instanceof SqlDelete) {
            lockUpHybridSearchNode(((SqlDelete)sqlNode).getCondition(), subSqlNode);
        }

    }

    public static void deepLockUpChildren(List<SqlNode> sqlNodes, ConcurrentHashMap<SqlBasicCall, SqlNode> subSqlNode) {
        if (sqlNodes == null) {
            return;
        }
        for (int i = 0; i < sqlNodes.size(); i ++) {
            lockUpHybridSearchNode(sqlNodes.get(i), subSqlNode);
        }
    }

    public static void deepLockUpChildren(List<SqlNode> sqlNodes, SqlNode subSqlNode) {
        if (sqlNodes == null) {
            return;
        }
        for (int i = 0; i < sqlNodes.size(); i ++) {
            lockUpHybridSearchNode(sqlNodes.get(i), subSqlNode);
        }
    }
}
