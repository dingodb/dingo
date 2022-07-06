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

package io.dingodb.sdk.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Filter;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Operation;
import io.dingodb.sdk.common.Processor;
import io.dingodb.sdk.common.RecordExistsAction;
import io.dingodb.sdk.common.Value;
import io.dingodb.sdk.configuration.ClassConfig;
import io.dingodb.sdk.configuration.Configuration;
import io.dingodb.sdk.utils.CheckUtils;
import io.dingodb.sdk.utils.ClassCache;
import io.dingodb.sdk.utils.ClassCacheEntry;
import io.dingodb.sdk.utils.DingoClientException;
import io.dingodb.sdk.utils.GenericTypeMapper;
import io.dingodb.sdk.utils.TypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.validation.constraints.NotNull;

@Slf4j
public class DingoOpCli implements DingoMapper {

    private final DingoClient dingoClient;
    private final MappingConverter mappingConverter;

    /**
     * will do retry when operation is failed.
     */
    boolean isRetryable = false;

    public static class Builder {
        private final DingoOpCli mapper;
        private List<Class<?>> classesToPreload = null;

        public Builder(DingoClient client) {
            this.mapper = new DingoOpCli(client);
        }

        /**
         * Add in a custom type converter.
         * The converter must have methods which implement the ToDingo and FromDingo annotation.
         *
         * @param converter The custom converter
         * @return this object
         */
        public Builder addConverter(Object converter) {
            GenericTypeMapper mapper = new GenericTypeMapper(converter);
            TypeUtils.addTypeMapper(mapper.getMappedClass(), mapper);
            return this;
        }

        public Builder preLoadClass(Class<?> clazz) {
            if (classesToPreload == null) {
                classesToPreload = new ArrayList<>();
            }
            classesToPreload.add(clazz);
            return this;
        }

        public Builder withConfigurationFile(File file) throws IOException {
            return this.withConfigurationFile(file, false);
        }

        public Builder withConfigurationFile(File file, boolean allowsInvalid) throws IOException {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Configuration configuration = objectMapper.readValue(file, Configuration.class);
            this.loadConfiguration(configuration, allowsInvalid);
            return this;
        }

        public Builder withConfigurationFile(InputStream ios) throws IOException {
            return this.withConfigurationFile(ios, false);
        }

        public Builder withConfigurationFile(InputStream ios, boolean allowsInvalid) throws IOException {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Configuration configuration = objectMapper.readValue(ios, Configuration.class);
            this.loadConfiguration(configuration, allowsInvalid);
            return this;
        }

        public Builder withConfiguration(String configurationYaml) throws JsonProcessingException {
            return this.withConfiguration(configurationYaml, false);
        }

        public Builder withConfiguration(
            String configurationYaml,
            boolean allowsInvalid) throws JsonProcessingException {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Configuration configuration = objectMapper.readValue(configurationYaml, Configuration.class);
            this.loadConfiguration(configuration, allowsInvalid);
            return this;
        }

        private void loadConfiguration(@NotNull Configuration configuration, boolean allowsInvalid) {
            for (ClassConfig config : configuration.getClasses()) {
                try {
                    String name = config.getClassName();
                    if (StringUtils.isBlank(name)) {
                        throw new DingoClientException("Class with blank name in configuration file");
                    } else {
                        try {
                            Class.forName(config.getClassName());
                        } catch (ClassNotFoundException e) {
                            throw new DingoClientException("Cannot find a class with name " + name);
                        }
                    }
                } catch (RuntimeException re) {
                    if (allowsInvalid) {
                        log.warn("Ignoring issue with configuration: " + re.getMessage());
                    } else {
                        throw re;
                    }
                }
            }
            ClassCache.getInstance().addConfiguration(configuration);
        }

        public DingoOpCli build() {
            if (classesToPreload != null) {
                for (Class<?> clazz : classesToPreload) {
                    ClassCache.getInstance().loadClass(clazz, this.mapper);
                }
            }
            return this.mapper;
        }
    }

    private DingoOpCli(@NotNull DingoClient client) {
        this.dingoClient = client;
        this.mappingConverter = new MappingConverter(this, dingoClient);
    }

    @Override
    public void save(@NotNull Object... objects) throws DingoClientException {
        for (Object thisObject : objects) {
            this.save(thisObject);
        }
    }

    @Override
    public void save(@NotNull Object object, String... binNames) throws DingoClientException {
        save(object, RecordExistsAction.REPLACE, binNames);
    }

    private <T> void save(@NotNull T object, RecordExistsAction recordExistsAction, String[] binNames) {
        Class<T> clazz = (Class<T>) object.getClass();
        ClassCacheEntry<T> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);

        String tableName = entry.getTableName();
        Key key = new Key(entry.getDatabase(), tableName, Arrays.asList(Value.get(entry.getKey(object))));
        Column[] columns = entry.getColumns(object, false, binNames);
        try {
            boolean isSuccess = dingoClient.put(key, columns);
            if (!isSuccess) {
                log.warn("Failed to save object " + object);
            }
        } catch (DingoClientException e) {
            throw e;
        } catch (Exception e) {
            throw new DingoClientException("Failed to save object " + object, e);
        }
    }

    @Override
    public void update(Object object, String... columnNames) {
        // todo
    }

    @Override
    public <T> T read(@NotNull Class<T> clazz, @NotNull Object userKey) throws DingoClientException {
        return this.read(clazz, userKey, true);
    }

    @Override
    public <T> T read(@NotNull Class<T> clazz,
                      @NotNull Object userKey,
                      boolean resolveDependencies) throws DingoClientException {
        ClassCacheEntry<T> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        /*
        Key key = new Key(entry.getDatabase(), tableName);
        return read(clazz, key, entry, resolveDependencies);
        */
        return null;
    }

    @Override
    public <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[] userKeys) throws DingoClientException {
        return read(clazz, userKeys);
    }

    @Override
    public <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[] userKeys, Operation... operations) {
        return read(clazz, userKeys, operations);
    }

    @Override
    public <T> boolean delete(@NotNull Class<T> clazz, @NotNull Object userKey) throws DingoClientException {
        return this.delete(clazz, userKey);
    }

    @Override
    public boolean delete(@NotNull Object object) throws DingoClientException {
        return this.delete(object);
    }

    @Override
    public <T> void find(@NotNull Class<T> clazz, Function<T, Boolean> function) throws DingoClientException {
        /*
        ClassCacheEntry<T> entry = MapperUtils.getEntryAndValidateNamespace(clazz, this);

        Statement statement = new Statement();
        statement.setNamespace(entry.getNamespace());
        statement.setSetName(entry.getSetName());

        RecordSet recordSet = null;
        try {
            // TODO: set the policy (If this statement is thought to be useful, which is dubious)
            recordSet = mClient.query(null, statement);
            T result;
            while (recordSet.next()) {
                result = clazz.getConstructor().newInstance();
                entry.hydrateFromRecord(recordSet.getRecord(), result);
                if (!function.apply(result)) {
                    break;
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new DingoClientException(e);
        } finally {
            if (recordSet != null) {
                recordSet.close();
            }
        }
         */
    }

    @Override
    public <T> void scan(@NotNull Class<T> clazz, @NotNull Processor<T> processor) {
        scan(clazz, processor);
    }

    @Override
    public <T> void scan(@NotNull Class<T> clazz, @NotNull Processor<T> processor, int recordsPerSecond) {
        // scan(null, clazz, processor, recordsPerSecond);
    }

    @Override
    public <T> List<T> scan(@NotNull Class<T> clazz) {
        return scan(clazz);
    }

    @Override
    public <T> void query(@NotNull Class<T> clazz, @NotNull Processor<T> processor, Filter filter) {
        // query(null, clazz, processor, filter);
    }

    @Override
    public <T> List<T> query(Class<T> clazz, Filter filter) {
        return query( clazz, filter);
    }

    @Override
    public DingoClient getClient() {
        return this.dingoClient;
    }

    @Override
    public MappingConverter getMappingConverter() {
        return this.mappingConverter;
    }

    @Override
    public DingoMapper asMapper() {
        return this;
    }
}
