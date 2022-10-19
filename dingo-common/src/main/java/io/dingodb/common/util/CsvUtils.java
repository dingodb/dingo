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

package io.dingodb.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterators;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.expr.json.runtime.Parser;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public final class CsvUtils {
    private static final Parser PARSER = Parser.CSV;

    private CsvUtils() {
    }

    public static @NonNull Iterator<String[]> readCsv(InputStream is) throws IOException {
        return PARSER.readValues(is, String[].class);
    }

    public static @NonNull Iterator<Object[]> readCsv(DingoType schema, InputStream is) throws IOException {
        return Iterators.transform(readCsv(is), i -> (Object[]) schema.parse(i));
    }

    public static @NonNull List<Object[]> readCsv(DingoType schema, String lines) throws JsonProcessingException {
        return Arrays.stream(PARSER.parse(lines, String[][].class))
            .map(i -> (Object[]) schema.parse(i))
            .collect(Collectors.toList());
    }

    /**
     * Read a csv stream into tuples. The first line is the tuple schema definition.
     *
     * @param is the csv stream
     * @return iterator of tuples
     * @throws IOException when errors occurred in reading the stream
     */
    public static @NonNull Iterator<Object[]> readCsvWithSchema(InputStream is) throws IOException {
        final Iterator<String[]> it = PARSER.readValues(is, String[].class);
        if (it.hasNext()) {
            String[] types = it.next();
            DingoType schema = DingoTypeFactory.tuple(types);
            return Iterators.transform(it, i -> (Object[]) schema.parse(i));
        }
        return Iterators.transform(it, s -> s);
    }
}
