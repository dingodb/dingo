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

package io.dingodb.test.dsl.run.check;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.test.asserts.ResultSetCheckConfig;
import io.dingodb.test.dsl.builder.checker.SqlCheckerVisitor;
import io.dingodb.test.dsl.builder.checker.SqlCsvFileNameResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlCsvFileResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlCsvStringResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlExceptionChecker;
import io.dingodb.test.dsl.builder.checker.SqlObjectResultChecker;
import io.dingodb.test.dsl.builder.checker.SqlResultCountChecker;
import io.dingodb.test.dsl.builder.checker.SqlResultDumper;
import io.dingodb.test.dsl.builder.checker.SqlUpdateCountChecker;
import io.dingodb.test.utils.CsvUtils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class SqlCheckerConverter implements SqlCheckerVisitor<Check> {
    private static final SqlCheckerConverter INSTANCE = new SqlCheckerConverter(null, null);

    private final Class<?> callerClass;
    private final String basePath;

    public static SqlCheckerConverter of(Class<?> callerClass, String basePath) {
        if (callerClass == null) {
            return INSTANCE;
        }
        return new SqlCheckerConverter(callerClass, basePath);
    }

    private static @NonNull Check createFromFile(InputStream csvFile, ResultSetCheckConfig config) {
        try {
            Iterator<String[]> it = CsvUtils.readCsv(csvFile);
            final String[] columnLabels = it.hasNext() ? it.next() : null;
            final DingoType schema = it.hasNext() ? DingoTypeFactory.INSTANCE.tuple(it.next()) : null;
            if (columnLabels == null || schema == null) {
                throw new IllegalArgumentException(
                    "Result file must be csv and its first two rows are column names and schema definitions."
                );
            }
            List<Object[]> tuples = ImmutableList.copyOf(
                Iterators.transform(it, i -> (Object[]) schema.parse(i))
            );
            return new ObjectResultCheck(columnLabels, tuples, config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Check visit(@NonNull SqlObjectResultChecker sqlChecker) {
        return new ObjectResultCheck(sqlChecker.getColumnLabels(), sqlChecker.getTuples(), sqlChecker.getConfig());
    }

    @Override
    public Check visit(@NonNull SqlResultCountChecker sqlChecker) {
        return new ResultCountCheck(sqlChecker.getRowCount());
    }

    @Override
    public Check visit(@NonNull SqlCsvStringResultChecker sqlChecker) {
        try (InputStream file = IOUtils.toInputStream(
            String.join("\n", sqlChecker.getCsvLines()),
            StandardCharsets.UTF_8
        )) {
            return createFromFile(file, sqlChecker.getConfig());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Check visit(@NonNull SqlCsvFileResultChecker sqlChecker) {
        return createFromFile(sqlChecker.getCsvFile(), sqlChecker.getConfig());
    }

    @Override
    public Check visit(@NonNull SqlCsvFileNameResultChecker sqlChecker) {
        try (InputStream file = callerClass.getResourceAsStream(basePath + "/" + sqlChecker.getFileName())) {
            return createFromFile(file, sqlChecker.getConfig());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Check visit(@NonNull SqlUpdateCountChecker sqlChecker) {
        return new UpdateCountCheck(sqlChecker.getUpdateCount());
    }

    @Override
    public Check visit(@NonNull SqlExceptionChecker sqlChecker) {
        return new ExceptionCheck(
            sqlChecker.getClazz(),
            sqlChecker.getContains(),
            sqlChecker.getSqlCode(),
            sqlChecker.getSqlState()
        );
    }

    @Override
    public Check visit(@NonNull SqlResultDumper sqlChecker) {
        return new DumpResult();
    }
}
