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

package io.dingodb.calcite.operation;

import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.dql.SqlShowGrants;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.PrivilegeList;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.verify.service.UserService;
import io.dingodb.verify.service.UserServiceProvider;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.util.PrivilegeUtils.getRealAddress;

@Slf4j
public class ShowGrantsOperation implements QueryOperation {
    @Setter
    public SqlNode sqlNode;
    static UserService userService = UserServiceProvider.getRoot();

    public ShowGrantsOperation(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
    }

    @Override
    public Iterator getIterator() {
        List<SqlGrant> sqlGrants = execute((SqlShowGrants) sqlNode);
        List<Object[]> showGrants = sqlGrants.stream().map(SqlNode::toString)
            .map(grant -> new Object[] {grant})
            .collect(Collectors.toList());
        return showGrants.iterator();
    }

    @Override
    public List<String> columns() {
        List<String> columns = new ArrayList<>();
        columns.add("grants");
        return columns;
    }

    public static List<SqlGrant> execute(@NonNull SqlShowGrants sqlShowGrants) {
        UserDefinition userDef = UserDefinition.builder()
            .user(sqlShowGrants.user)
            .host(getRealAddress(sqlShowGrants.host))
            .build();
        if (!userService.existsUser(userDef)) {
            throw new RuntimeException("user is not exist");
        }
        PrivilegeGather privilegeGather = userService.getPrivilegeDef(sqlShowGrants.user,
            getRealAddress(sqlShowGrants.host));
        List<SchemaPrivDefinition> schemaPrivDefinitions = new ArrayList<>(privilegeGather
            .getSchemaPrivDefMap().values());
        UserDefinition userDefinition = privilegeGather.getUserDef();

        if (userDefinition == null) {
            return new ArrayList<>();
        }

        List<SqlGrant> sqlGrants = new ArrayList<>();
        SqlGrant userGrant;
        if ((userGrant = getUserGrant(sqlShowGrants, userDefinition)) != null) {
            sqlGrants.add(userGrant);
        }
        sqlGrants.addAll(getSchemaGrant(sqlShowGrants, schemaPrivDefinitions));
        List<TablePrivDefinition> tablePrivDefinitions = new ArrayList<>(privilegeGather
            .getTablePrivDefMap().values());
        sqlGrants.addAll(getTableGrant(sqlShowGrants, tablePrivDefinitions));
        return sqlGrants;
    }

    public static SqlGrant getUserGrant(@NonNull SqlShowGrants dingoSqlShowGrants, UserDefinition userDefinition) {
        List<Boolean> userPrivileges = Arrays.asList(userDefinition.getPrivileges());
        long count = userPrivileges.stream()
            .filter(isPrivilege -> isPrivilege).count();
        boolean withGrantOption = userPrivileges.get(8);
        if (count > 0) {
            boolean isAllPrivilege = false;
            List<String> privileges;

            if (count == PrivilegeList.privilegeMap.get(PrivilegeType.USER).size()) {
                isAllPrivilege = true;
                privileges = new ArrayList<>(PrivilegeList.privilegeMap.get(PrivilegeType.USER));
            } else {
                List<Integer> indexs = new ArrayList<>();
                Stream.iterate(0, i -> i + 1).limit(userPrivileges.size()).forEach(i -> {
                    if (userPrivileges.get(i)) {
                        indexs.add(i);
                    }
                });
                privileges = PrivilegeDict.getPrivilege(indexs);
            }
            SqlParserPos pos = new SqlParserPos(0, 0);
            SqlIdentifier subject = new SqlIdentifier(Arrays.asList("*", "*"), null,
                pos,
                new ArrayList<>());
            SqlGrant sqlGrant = new SqlGrant(pos, isAllPrivilege, privileges, subject,
                dingoSqlShowGrants.user, dingoSqlShowGrants.host, withGrantOption);
            log.info("user sqlGrant:" + sqlGrant);
            return sqlGrant;
        }
        return null;
    }

    public static List<SqlGrant> getSchemaGrant(@NonNull SqlShowGrants sqlShowGrants,
                                         List<SchemaPrivDefinition> schemaPrivDefinitions) {
        List<SqlGrant> sqlGrants = new ArrayList<>();
        for (SchemaPrivDefinition schemaPrivDefinition : schemaPrivDefinitions) {
            List<Boolean> schemaPrivileges = Arrays.asList(schemaPrivDefinition.getPrivileges());
            boolean withGrantOption = schemaPrivileges.get(8);
            long count = schemaPrivileges.stream()
                .filter(isPrivilege -> isPrivilege).count();

            if (count > 0) {
                boolean isAllPrivilege = false;
                List<String> privileges;
                if (count == PrivilegeList.privilegeMap.get(PrivilegeType.SCHEMA).size()) {
                    isAllPrivilege = true;
                    privileges = new ArrayList<>(PrivilegeList.privilegeMap.get(PrivilegeType.SCHEMA));
                } else {
                    List<Integer> indexs = new ArrayList<>();
                    Stream.iterate(0, i -> i + 1).limit(schemaPrivileges.size()).forEach(i -> {
                        if (schemaPrivileges.get(i)) {
                            indexs.add(i);
                        }
                    });
                    privileges = PrivilegeDict.getPrivilege(indexs);
                }
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                SqlIdentifier subject = new SqlIdentifier(Arrays.asList(schemaPrivDefinition.getSchemaName(), "*"),
                    null, sqlParserPos,
                    new ArrayList<>());
                SqlGrant sqlGrant = new SqlGrant(sqlParserPos, isAllPrivilege, privileges, subject,
                    sqlShowGrants.user, sqlShowGrants.host, withGrantOption);
                log.info("schema sqlGrant:" + sqlGrant);
                sqlGrants.add(sqlGrant);
            }
        }
        return sqlGrants;
    }

    public static List<SqlGrant> getTableGrant(@NonNull SqlShowGrants sqlShowGrants,
                                        List<TablePrivDefinition> tablePrivDefinitions) {
        List<SqlGrant> sqlGrants = new ArrayList<>();
        for (TablePrivDefinition tablePrivDefinition : tablePrivDefinitions) {
            if (tablePrivDefinition == null) {
                continue;
            }
            List<Boolean> userPrivileges = Arrays.asList(tablePrivDefinition.getPrivileges());
            boolean withGrantOption = userPrivileges.get(8);
            long count = userPrivileges.stream()
                .filter(isPrivilege -> isPrivilege).count();

            if (count > 0) {
                boolean isAllPrivilege = false;
                List<String> privileges;
                if (count == PrivilegeList.privilegeMap.get(PrivilegeType.TABLE).size()) {
                    isAllPrivilege = true;
                    privileges = new ArrayList<>(PrivilegeList.privilegeMap.get(PrivilegeType.TABLE));
                } else {
                    List<Integer> indexs = new ArrayList<>();
                    Stream.iterate(0, i -> i + 1).limit(userPrivileges.size()).forEach(i -> {
                        if (userPrivileges.get(i)) {
                            indexs.add(i);
                        }
                    });
                    privileges = PrivilegeDict.getPrivilege(indexs);
                }
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                SqlIdentifier subject = new SqlIdentifier(Arrays.asList(tablePrivDefinition.getSchemaName(),
                    tablePrivDefinition.getTableName()), null, sqlParserPos,
                    new ArrayList<>());
                SqlGrant sqlGrant = new SqlGrant(sqlParserPos, isAllPrivilege, privileges, subject,
                    sqlShowGrants.user, sqlShowGrants.host, withGrantOption);
                log.info("table sqlGrant:" + sqlGrant);
                sqlGrants.add(sqlGrant);
            }
        }
        return sqlGrants;
    }
}
