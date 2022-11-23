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

package io.dingodb.expr.json.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;

public class Parser implements Serializable {
    public static final Parser JSON = new Parser(DataFormat.APPLICATION_JSON);
    public static final Parser YAML = new Parser(DataFormat.APPLICATION_YAML);
    public static final Parser CSV = new Parser(DataFormat.TEXT_CSV);

    private static final long serialVersionUID = -4801322278537134701L;
    protected final ObjectMapper mapper;

    static {

    }

    protected Parser(@NonNull DataFormat format) {
        switch (format) {
            case APPLICATION_JSON:
                mapper = JsonMapper.builder()
                    .addModule(new AfterburnerModule())
                    .build();
                setJsonFeature(mapper);
                break;
            case APPLICATION_YAML:
                YAMLFactory yamlFactory = new YAMLFactory()
                    .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
                mapper = JsonMapper.builder(yamlFactory)
                    .addModule(new AfterburnerModule())
                    .build();
                setJsonFeature(mapper);
                break;
            case TEXT_CSV:
                mapper = setCsvFeature(new CsvMapper());
                break;
            default:
                throw new IllegalArgumentException("Invalid DataFormat value \"" + format
                    + "\" for ParserFactory.");
        }
    }

    private static void setJsonFeature(@NonNull ObjectMapper mapper) {
        mapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
        mapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
        mapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        mapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
        mapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
        mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.enable(DeserializationFeature.USE_LONG_FOR_INTS);
    }

    private static @NonNull CsvMapper setCsvFeature(@NonNull CsvMapper mapper) {
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        mapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);
        mapper.enable(CsvParser.Feature.TRIM_SPACES);
        return mapper;
    }

    public static Parser get(@NonNull DataFormat format) {
        switch (format) {
            case APPLICATION_JSON:
                return JSON;
            case APPLICATION_YAML:
                return YAML;
            case TEXT_CSV:
                return CSV;
            default:
                throw new IllegalArgumentException("Unsupported format \"" + format + "\".");
        }
    }

    public static void registerSubtypes(Class<?>... classes) {
        JSON.mapper.registerSubtypes(classes);
        YAML.mapper.registerSubtypes(classes);
        CSV.mapper.registerSubtypes(classes);
    }

    public <T> T parse(String json, Class<T> clazz) throws JsonProcessingException {
        return mapper.readValue(json, clazz);
    }

    public <T> T parse(InputStream is, Class<T> clazz) throws IOException {
        return mapper.readValue(is, clazz);
    }

    public <T> String stringify(T obj) throws JsonProcessingException {
        return mapper.writeValueAsString(obj);
    }

    public <T> void writeStream(OutputStream os, T obj) throws IOException {
        mapper.writeValue(os, obj);
    }

    public <T> Iterator<T> readValues(InputStream is, Class<T> clazz) throws IOException {
        return mapper.readerFor(clazz).readValues(is);
    }
}
