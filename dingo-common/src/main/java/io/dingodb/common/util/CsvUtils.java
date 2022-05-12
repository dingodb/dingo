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
import io.dingodb.common.table.TupleSchema;
import io.dingodb.expr.json.runtime.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public final class CsvUtils {
    private static final Parser PARSER = Parser.CSV;

    private CsvUtils() {
    }

    @Nonnull
    public static Iterator<String[]> readCsv(InputStream is) throws IOException {
        return PARSER.readValues(is, String[].class);
    }

    @Nonnull
    public static Iterator<Object[]> readCsv(@Nonnull TupleSchema schema, InputStream is) throws IOException {
        return Iterators.transform(readCsv(is), schema::parse);
    }

    @Nonnull
    public static List<Object[]> readCsv(@Nonnull TupleSchema schema, String lines) throws JsonProcessingException {
        return Arrays.stream(PARSER.parse(lines, String[][].class))
            .map(schema::parse)
            .collect(Collectors.toList());
    }

    /**
     * Read a csv stream into tuples. The first line is the tuple schema definition.
     *
     * @param is the csv stream
     * @return iterator of tuples
     * @throws IOException when errors occurred in reading the stream
     */
    @Nonnull
    public static Iterator<Object[]> readCsvWithSchema(InputStream is) throws IOException {
        final Iterator<String[]> it = PARSER.readValues(is, String[].class);
        if (it.hasNext()) {
            String[] types = it.next();
            TupleSchema schema = TupleSchema.ofTypes(types);
            return Iterators.transform(it, schema::parse);
        }
        return Iterators.transform(it, s -> s);
    }
}
