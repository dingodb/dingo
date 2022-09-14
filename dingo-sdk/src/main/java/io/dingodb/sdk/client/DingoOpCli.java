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
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.operation.ParticleType;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.filter.DingoFilter;
import io.dingodb.common.operation.filter.impl.DingoNumberRangeFilter;
import io.dingodb.common.operation.filter.impl.DingoStringRangeFilter;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Filter;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Processor;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.configuration.ClassConfig;
import io.dingodb.sdk.configuration.Configuration;
import io.dingodb.sdk.utils.CheckUtils;
import io.dingodb.sdk.utils.ClassCache;
import io.dingodb.sdk.utils.ClassCacheEntry;
import io.dingodb.sdk.utils.GenericTypeMapper;
import io.dingodb.sdk.utils.LoadedObjectResolver;
import io.dingodb.sdk.utils.ThreadLocalKeySaver;
import io.dingodb.sdk.utils.TypeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.validation.constraints.NotNull;

@Slf4j
public class DingoOpCli implements DingoMapper {

    private final DingoClient dingoClient;
    private final MappingConverter mappingConverter;

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

    public boolean createTable(Class<?> clazz) throws DingoClientException {
        ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Cannot find table name for class " + clazz.getName());
        }
        TableDefinition tableDefinition = entry.getTableDefinition(tableName);
        if (tableDefinition == null || tableDefinition.getColumns().isEmpty()) {
            throw new DingoClientException("Cannot find table definition for class " + clazz.getName());
        }

        boolean hasPrimaryKey = false;
        hasPrimaryKey = tableDefinition.getColumns().stream().anyMatch(col -> col.isPrimary());
        if (!hasPrimaryKey) {
            throw new DingoClientException("Table " + tableName + " does not have a primary key");
        }

        boolean isSuccess = false;
        try {
            isSuccess = dingoClient.createTable(tableDefinition);
            if (!isSuccess) {
                log.warn("Failed to create table:{}", tableName);
            }
        } catch (DingoClientException ex) {
            log.error("Failed to create table:{} define:{} catch exception:{}",
                tableName,
                tableDefinition,
                ex.toString(), ex);
            throw ex;
        } catch (Exception e) {
            throw new DingoClientException("Failed to create table:" + tableName);
        }
        return isSuccess;
    }

    public boolean dropTable(Class<?> clazz) {
        ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        return dropTable(tableName);
    }

    public boolean dropTable(String tableName) {
        boolean isSuccess = false;
        try {
            isSuccess = dingoClient.dropTable(tableName);
            if (!isSuccess) {
                log.warn("Failed to drop table:{}", tableName);
            }
        } catch (DingoClientException ex) {
            log.error("Failed to drop table:{} catch exception:{}",
                tableName,
                ex.toString(), ex);
            throw ex;
        } catch (Exception e) {
            throw new DingoClientException("Failed to drop table:" + tableName);
        }
        return isSuccess;
    }

    @Override
    public void save(@NotNull Object[] objectArray) throws DingoClientException {
        List<Key> keyList = new ArrayList<>();
        List<Record> recordList = new ArrayList<>();

        boolean isSameType = true;
        String previousClassName = " ";
        if (objectArray.length > 0) {
            previousClassName = objectArray[0].getClass().getName();
        }

        for (Object object : objectArray) {
            if (!object.getClass().getName().equals(previousClassName)) {
                isSameType = false;
                break;
            }

            ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(object.getClass(), this);
            String tableName = entry.getTableName();
            if (tableName == null || tableName.isEmpty()) {
                throw new DingoClientException("Cannot find table name for class " + object.getClass().getName());
            }

            TableDefinition tableDefinition = entry.getTableDefinition(tableName);
            if (tableDefinition == null) {
                throw new DingoClientException("Cannot find table name for class " + object.getClass().getName());
            }

            Key key = new Key(entry.getDatabase(), tableName, Arrays.asList(Value.get(entry.getKey(object))));
            Column[] columns = entry.getColumns(object, true);
            Record record = new Record(tableDefinition.getColumns(), columns);

            keyList.add(key);
            recordList.add(record);
        }

        if (!isSameType || keyList.size() != 0 && keyList.size() != recordList.size()) {
            throw new DingoClientException("Cannot save objects with different types");
        }

        doSave(keyList, recordList);
    }

    @Override
    public void save(@NotNull Object object) throws DingoClientException {
        ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(object.getClass(), this);
        String tableName = entry.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Cannot find table name for class " + object.getClass().getName());
        }

        TableDefinition tableDefinition = entry.getTableDefinition(tableName);
        if (tableDefinition == null) {
            throw new DingoClientException("Cannot find table name for class " + object.getClass().getName());
        }

        List<Value> keyList = new ArrayList<>();
        List entryKey = (List) entry.getKey(object);
        for (Object o : entryKey) {
            keyList.add(Value.get(o));
        }
        Key key = new Key(entry.getDatabase(), tableName, keyList);
        Column[] columns = entry.getColumns(object, true);
        Record record = new Record(tableDefinition.getColumns(), columns);

        doSave(Arrays.asList(key), Arrays.asList(record));
    }

    private void doSave(@NotNull List<Key> keyList, List<Record> recordList) {
        try {
            boolean isSuccess = dingoClient.put(keyList, recordList);
            if (!isSuccess) {
                log.warn("Failed to Save objects in batch: cnt:{}", keyList.size());
            }
        } catch (DingoClientException ex) {
            log.error("Failed to Save objects in batch: cnt:{} catch exception:{}",
                keyList.size(),
                ex.toString(), ex);
            throw ex;
        } catch (Exception ex) {
            throw new DingoClientException("Failed to Save objects in batch: cnt:" + keyList.size(), ex);
        }
    }


    /**
     * get the stored format of the object.
     *
     * @param object input Object
     * @return columns about the table
     * @throws DingoClientException dingo client exception
     *                              Use case: TestCases to get the stored format of the object.
     */
    public Column[] getColumnsSeqInStore(Object object) throws DingoClientException {
        Class<?> clazz = object.getClass();
        ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Cannot find table name for class " + clazz.getName());
        }
        Column[] columns = entry.getColumns(object, true);
        return columns;
    }

    @Override
    public boolean update(Object object, String... columnNames) {
        Class<?> clazz = (Class<?>) object.getClass();
        ClassCacheEntry<?> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);

        /**
         * check input column name is valid.
         */
        if (entry == null || entry.isAllColumnsValid(columnNames)) {
            throw new DingoClientException("Invalid column name:" + Arrays.toString(columnNames));
        }

        boolean isSuccess = false;
        String tableName = entry.getTableName();
        Key key = new Key(entry.getDatabase(), tableName, Arrays.asList(Value.get(entry.getKey(object))));
        try {
            Record oldRecord = dingoClient.get(key);
            if (oldRecord != null) {
                Column[] columns = entry.getColumns(object, oldRecord, true, columnNames);
                isSuccess = dingoClient.put(key, columns);
            } else {
                log.warn("Check key:{} not existed on table:{}. Write whole record directly", key, tableName);
                Column[] writeColumns = entry.getColumns(object, true);
                isSuccess = dingoClient.put(key, writeColumns);
            }
            return isSuccess;
        } catch (Exception e) {
            log.error("Failed to update key:{} on table:{}", key, tableName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T read(@NotNull Class<T> clazz, @NotNull Object[] userKeys) throws DingoClientException {
        if (clazz == null || userKeys == null) {
            throw new DingoClientException("Class or Key is null");
        }
        ClassCacheEntry<T> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        List<Value> valueList = new ArrayList<>();
        for (Object o : userKeys) {
            valueList.add(Value.get(o));
        }
        Key key = new Key(entry.getDatabase(), tableName, valueList);
        try {
            Record record = dingoClient.get(key);
            ThreadLocalKeySaver.save(key);
            LoadedObjectResolver.begin();
            return mappingConverter.convertToObject(clazz, record, entry, true);
        } catch (DingoClientException ex) {
            log.error("Get Key:{} on table:{} catch exception:{}",
                key, tableName,
                ex.toString(), ex);
            throw ex;
        } catch (Exception e) {
            throw new DingoClientException("Failed to get object:{}" + userKeys, e);
        } finally {
            LoadedObjectResolver.end();
            ThreadLocalKeySaver.clear();
        }
    }

    @Override
    public <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[][] userKeys) throws DingoClientException {
        if (clazz == null || userKeys == null || userKeys.length == 0) {
            throw new DingoClientException("Class or keys is null");
        }

        T[] result = (T[]) Array.newInstance(clazz, userKeys.length);
        for (int i = 0; i < userKeys.length; i++) {
            result[i] = read(clazz, userKeys[i]);
        }
        return result;
    }

    @Override
    public <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[] userKeys, Operation... operations) {
        return read(clazz, userKeys, operations);
    }

    @Override
    public boolean delete(Key userKey) throws DingoClientException {
        String tableName = "";
        try {
            if (userKey == null) {
                log.warn("Delete Key:{} is empty on table:{}", userKey, tableName);
            }
            tableName = userKey.getTable();
            boolean isSuccess = dingoClient.delete(userKey);
            if (!isSuccess) {
                log.warn("Failed to delete object:{} on table:{}", userKey, tableName);
            }
            return isSuccess;
        } catch (DingoClientException ex) {
            log.error("Delete Key:{} on table:{} catch exception:{}",
                userKey,
                tableName,
                ex.toString(), ex);
            throw ex;
        } catch (Exception e) {
            throw new DingoClientException("Failed to delete object:{}" + userKey);
        }
    }

    @Override
    public boolean delete(@NotNull Object object) throws DingoClientException {
        Class<Object> clazz = (Class<Object>) object.getClass();
        ClassCacheEntry<Object> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Table name is null");
        }
        Key key = new Key(entry.getDatabase(), tableName, Arrays.asList(Value.get(entry.getKey(object))));
        return delete(key);
    }

    @Override
    public <T> void query(@NotNull Class<T> clazz,
                          @NotNull Processor<T> processor,
                          Filter filter) {
        ClassCacheEntry<T> entry = CheckUtils.getEntryAndValidateTableName(clazz, this);
        String tableName = entry.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Table name is null");
        }

        if (filter == null
            || filter.getColumnValueStart() == null
            || filter.getColumnValueEnd() == null) {
            String outMsg = (filter == null) ? "null" : filter.toString();
            throw new DingoClientException("Invalid Input Filter:" + outMsg);
        }

        TableDefinition tableDefinition = entry.getTableDefinition(tableName);
        if (tableDefinition == null) {
            throw new DingoClientException("Cannot find table name for class " + clazz.getName());
        }

        int columnIndex = getColumnIndexByName(tableDefinition, filter.getColumnName());
        if (columnIndex == -1) {
            String errorMsg = "Cannot find columnName:" + filter.getColumnName() + " in table " + tableName;
            throw new DingoClientException(errorMsg);
        }

        Key startKey = new Key(tableName, Arrays.asList(filter.getStartKey()));
        Key endKey = new Key(tableName, Arrays.asList(filter.getEndKey()));

        String startRange = filter.getColumnValueStart().toString();
        String endRange = filter.getColumnValueEnd().toString();

        DingoFilter dingoFilter = null;
        if (filter.getColumnValueStart().getType() == ParticleType.STRING) {
            dingoFilter = new DingoStringRangeFilter(
                columnIndex,
                startRange,
                endRange
            );
        } else {
            dingoFilter = new DingoNumberRangeFilter(
                columnIndex,
                Double.valueOf(startRange),
                Double.valueOf(endRange)
            );
        }

        List<Record> recordList = dingoClient.query(startKey, endKey, dingoFilter);
        try {
            if (recordList == null) {
                log.warn("Execute query:{} on table:{} get empty record list",
                    filter.toString(),
                    tableName
                );
                return;
            }

            for (Record record : recordList) {
                T object = this.getMappingConverter().convertToObject(clazz, record);
                if (!processor.process(object)) {
                    break;
                }
            }
        } catch (DingoClientException ex) {
            log.error("Query:{} in table:{} catch exception:{}",
                filter.toString(), tableName, ex.toString(), ex);
        }
    }


    @Override
    public <T> List<T> query(Class<T> clazz, Filter filter) {
        List<T> result = new ArrayList<>();
        Processor<T> resultProcessor = record -> {
            result.add(record);
            return true;
        };
        query(clazz, resultProcessor, filter);
        return result;
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

    private int getColumnIndexByName(TableDefinition tableDefinition, String columnName) {
        int index = 0;
        boolean isFound = false;
        for (ColumnDefinition columnDef : tableDefinition.getColumns()) {
            if (columnDef.getName().equalsIgnoreCase(columnName)) {
                isFound = true;
                break;
            }
            index++;
        }
        return isFound ? index : -1;
    }
}
