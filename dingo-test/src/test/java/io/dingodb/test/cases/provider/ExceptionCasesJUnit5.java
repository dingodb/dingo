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

package io.dingodb.test.cases.provider;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ExceptionCasesJUnit5 implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Stream.of(
            // Parsing error
            arguments("SQL Parse error", ImmutableList.of(
                "select"
            ), 51001, "51001", false),
            arguments("Illegal expression in context", ImmutableList.of(
                "insert into {table} (1)"
            ), 51002, "51002", false),
            // DDL error
            arguments("Unknown identifier", ImmutableList.of(
                "create table {table} (id int, data bomb, primary key(id))"
            ), 52001, "52001", false),
            arguments("Table already exists", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "create table {table} (id int, primary key(id))"
            ), 52002, "52002", true),
            arguments("Create without primary key", ImmutableList.of(
                "create table {table} (id int)"
            ), 52003, "52003", false),
            arguments("Missing column list", ImmutableList.of(
                "create table {table}"
            ), 52004, "52004", false),
            // validation error
            arguments("Table not found", ImmutableList.of(
                "select * from {table}"
            ), 53001, "53001", false),
            arguments("Column not found", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "select data from {table}"
            ), 53002, "53002", true),
            arguments("Column not allow null", ImmutableList.of(
                "create table {table} (id int, primary key(id))",
                "insert into {table} values(null)"
            ), 53003, "53003", true),
            arguments("Column not allow null (array)", ImmutableList.of(
                "create table {table} (id int, data varchar array not null, primary key(id))",
                "insert into {table} values(1, null)"
            ), 53003, "53003", true),
            arguments("Column not allow null (map)", ImmutableList.of(
                "create table {table} (id int, data map not null, primary key(id))",
                "insert into {table} values(1, null)"
            ), 53003, "53003", true),
            arguments("Number format error", ImmutableList.of(
                "create table {table} (id int, data int, primary key(id))",
                "insert into {table} values(1, 'abc')"
            ), 53004, "53004", false),
            // execution error
            arguments("Task failed", ImmutableList.of(
                "create table {table} (id int, data double, primary key(id))",
                "insert into {table} values (1, 3.5)",
                "update {table} set data = 'abc'"
            ), 60000, "60000", true),
            // Unknown
            arguments("Cast float to int", ImmutableList.of(
                "select cast(18293824503.55 as int) ca"
            ), 90001, "90001", false),
            // intentionally
            arguments("By `thrown` function", ImmutableList.of(
                "select throw(null)"
            ), 90002, "90002", false)
        );
    }
}
