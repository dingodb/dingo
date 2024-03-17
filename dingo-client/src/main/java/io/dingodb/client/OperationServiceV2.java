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

package io.dingodb.client;

import com.google.common.collect.ImmutableList;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Record;
import io.dingodb.client.operation.impl.DeleteRangeResult;
import io.dingodb.client.operation.impl.OpKeyRange;
import io.dingodb.client.utils.OperationUtils;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.DingoTypeFactory;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Optional;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.impl.IdGeneratorImpl;
import io.dingodb.exec.impl.JobIteratorImpl;
import io.dingodb.exec.impl.JobManagerImpl;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.operator.params.CoalesceParam;
import io.dingodb.exec.operator.params.CompareAndSetParam;
import io.dingodb.exec.operator.params.CopyParam;
import io.dingodb.exec.operator.params.DistributionParam;
import io.dingodb.exec.operator.params.DistributionSourceParam;
import io.dingodb.exec.operator.params.GetByKeysParam;
import io.dingodb.exec.operator.params.GetDistributionParam;
import io.dingodb.exec.operator.params.PartDeleteParam;
import io.dingodb.exec.operator.params.PartInsertParam;
import io.dingodb.exec.operator.params.PartRangeDeleteParam;
import io.dingodb.exec.operator.params.PartRangeScanParam;
import io.dingodb.exec.operator.params.RootParam;
import io.dingodb.exec.operator.params.SumUpParam;
import io.dingodb.exec.operator.params.TxnGetByKeysParam;
import io.dingodb.exec.operator.params.TxnPartDeleteParam;
import io.dingodb.exec.operator.params.TxnPartInsertParam;
import io.dingodb.exec.operator.params.ValuesParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.exec.transaction.base.TransactionType;
import io.dingodb.exec.transaction.impl.TransactionManager;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.meta.entity.Table;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.store.api.transaction.data.IsolationLevel;
import io.dingodb.store.proxy.service.CodecService;
import io.dingodb.store.proxy.service.TsoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.client.utils.OperationUtils.mapKey2;
import static io.dingodb.client.utils.OperationUtils.mapKeyPrefix;
import static io.dingodb.common.util.Utils.sole;
import static io.dingodb.exec.utils.OperatorCodeUtils.CALC_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.COALESCE;
import static io.dingodb.exec.utils.OperatorCodeUtils.COMPARE_AND_SET;
import static io.dingodb.exec.utils.OperatorCodeUtils.COPY;
import static io.dingodb.exec.utils.OperatorCodeUtils.DISTRIBUTE;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_BY_KEYS;
import static io.dingodb.exec.utils.OperatorCodeUtils.GET_DISTRIBUTION;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.PART_RANGE_SCAN;
import static io.dingodb.exec.utils.OperatorCodeUtils.ROOT;
import static io.dingodb.exec.utils.OperatorCodeUtils.SUM_UP;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_GET_BY_KEYS;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_DELETE;
import static io.dingodb.exec.utils.OperatorCodeUtils.TXN_PART_INSERT;
import static io.dingodb.exec.utils.OperatorCodeUtils.VALUES;

@Slf4j
public class OperationServiceV2 {

    private final MetaService metaService;
    private JobManagerImpl jobManager;

    public OperationServiceV2(String coordinatorSvr) {
        DingoConfiguration.instance().getConfigMap("store").put("coordinators", coordinatorSvr);
        DingoConfiguration.instance().setServerId(new CommonId(CommonId.CommonType.SDK, 1, 1));
        metaService = MetaService.root();
        jobManager = JobManagerImpl.INSTANCE;
    }

    public void close() {

    }

    public MetaService getSubMetaService(String schemaName) {
        schemaName = schemaName.toUpperCase();
        return Parameters.nonNull(metaService.getSubMetaService(schemaName), "Schema not found: " + schemaName);
    }

    public Table getTable(String schema, String table) {
        return getSubMetaService(schema).getTable(table);
    }

    private long tso() {
        return TsoService.INSTANCE.tso();
    }

    private ITransaction getTransaction(String schema, String tableName) {
        MetaService metaService = getSubMetaService(schema);
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        if (table.engine != null && table.engine.contains("TXN")) {
            long startTs = TransactionManager.getStartTs();
            ITransaction transaction = TransactionManager.createTransaction(
                TransactionType.OPTIMISTIC,
                startTs,
                IsolationLevel.SnapshotIsolation.getCode());
            Properties properties = new Properties();
            properties.setProperty("lock_wait_timeout", "50");
            properties.setProperty("transaction_isolation", "REPEATABLE-READ");
            properties.setProperty("transaction_read_only", "off");
            properties.setProperty("txn_mode", "optimistic");
            properties.setProperty("collect_txn", "true");
            properties.setProperty("statement_timeout", "50000");
            properties.setProperty("txn_inert_check", "off");
            properties.setProperty("txn_retry", "off");
            properties.setProperty("txn_retry_cnt", "0");
            transaction.setTransactionConfig(properties);
            return transaction;
        }
        return null;
    }

    public DeleteRangeResult rangeDelete(String schema,
                                         String tableName,
                                         Key begin,
                                         Key end,
                                         boolean withBegin,
                                         boolean withEnd
    ) {
        long jobSeqId = tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        CommonId jobId = job.getJobId();
        try {
            Location currentLocation = MetaService.root().currentLocation();
            // rangeDelete --> root
            List<Vertex> rangeDeleteOutputs = rangeDelete(
                schema,
                tableName,
                job,
                idGenerator,
                currentLocation,
                begin,
                end,
                withBegin,
                withEnd);
            List<Vertex> root = root(job, idGenerator, currentLocation, rangeDeleteOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            Long count = 0L;
            while (iterator.hasNext()) {
                count += (Long) iterator.next()[0];
            }
            return new DeleteRangeResult(count, null);
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    public List<Record> get(String schema, String tableName, List<Key> keys) {
        long jobSeqId = tso();
        ITransaction transaction = getTransaction(schema, tableName);
        Job job;
        if (transaction != null) {
            job = jobManager.createJob(jobSeqId, jobSeqId, transaction.getTxnId(), null);
        } else {
            job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        }
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        CommonId jobId = job.getJobId();
        MetaService metaService = getSubMetaService(schema);
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        List<Column> columns = table.getColumns();
        List<Object[]> tuples = keys.stream()
            .map(k -> mapKey2(k.getUserKey().toArray(), new Object[columns.size()], columns, table.keyColumns()))
            .collect(Collectors.toList());

        try {
            Location currentLocation = MetaService.root().currentLocation();
            // distribution --> getByKey --> root
            List<Vertex> byKeyOutputs = getByKey(table, job, idGenerator, currentLocation, transaction, tuples);
            List<Vertex> root = root(job, idGenerator, currentLocation, byKeyOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            List<Record> records = new ArrayList<>();
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
                records.add(new Record(next, table.getColumns()));
            }
            return records;
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    public Boolean[] delete(String schema, String tableName, List<Key> keys) {
        long jobSeqId = tso();
        ITransaction transaction = getTransaction(schema, tableName);
        Job job;
        if (transaction != null) {
            job = jobManager.createJob(jobSeqId, jobSeqId, transaction.getTxnId(), null);
        } else {
            job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        }
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        CommonId jobId = job.getJobId();

        MetaService metaService = getSubMetaService(schema);
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        List<Column> columns = table.getColumns();
        List<Object[]> tuples = keys.stream()
            .map(k ->
                mapKey2(k.getUserKey().toArray(), new Object[columns.size()], columns, table.keyColumns()))
            .collect(Collectors.toList());
        try {
            Location currentLocation = MetaService.root().currentLocation();
            // values --> delete --> coalesce --> sumUp --> root
            List<Vertex> getByKeyOutputs = getByKey(table, job, idGenerator, currentLocation, transaction, tuples);
            List<Vertex> copyOutputs = copy(table, job, idGenerator, currentLocation, transaction, getByKeyOutputs);
            List<Vertex> coalesceOutputs = coalesce(idGenerator, copyOutputs);
            List<Vertex> deleteOutputs = delete(table, idGenerator, currentLocation, transaction, coalesceOutputs);
            List<Vertex> root = root(job, idGenerator, currentLocation, deleteOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            // Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            Task task = job.getTasks().values().iterator().next();
            if (task.getRoot() != null) {
                jobManager.getTaskManager().addTask(task);
                task.run(null);
            }
            Task rootTask = job.getRoot();
            // Iterator<Object[]> iterator = new JobIteratorImpl(job, rootTask.getRoot());
            Boolean[] keyState = task.getContext().getKeyState();
            if (transaction != null) {
                transaction.addSql("insert");
                transaction.commit(jobManager);
            }
            return keyState;
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    public Boolean[] insert(String schema, String tableName, List<Object[]> tuples) {
        long jobSeqId = tso();
        ITransaction transaction = getTransaction(schema, tableName);
        Job job;
        if (transaction != null) {
            job = jobManager.createJob(jobSeqId, jobSeqId, transaction.getTxnId(), null);
        } else {
            job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        }
        IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        MetaService metaService = getSubMetaService(schema);
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        CommonId jobId = job.getJobId();
        try {
            Location currentLocation = MetaService.root().currentLocation();
            // values --> partition --> insert --> coalesce --> sumUp --> root
            List<Vertex> valuesOutputs = values(
                job,
                idGenerator,
                currentLocation,
                tuples);
            List<Vertex> partitionOutputs = copy(
                table,
                job,
                idGenerator,
                currentLocation,
                transaction,
                valuesOutputs);
            List<Vertex> insertOutputs = insert(
                table,
                idGenerator,
                currentLocation,
                transaction,
                partitionOutputs);
            List<Vertex> coalesceOutputs = coalesce(
                idGenerator,
                insertOutputs);
            List<Vertex> root = root(
                job,
                idGenerator,
                currentLocation,
                coalesceOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            // Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            Task task = job.getTasks().values().iterator().next();
            if (task.getRoot() != null) {
                jobManager.getTaskManager().addTask(task);
                task.run(null);
            }
            Task rootTask = job.getRoot();
            // Iterator<Object[]> iterator = new JobIteratorImpl(job, rootTask.getRoot());
            Boolean[] keyState = task.getContext().getKeyState();
            if (transaction != null) {
                transaction.addSql("insert");
                transaction.commit(jobManager);
            }
            return keyState;
        } finally {
            jobManager.removeJob(jobId);
            if (transaction != null) {
                transaction.close(jobManager);
            }
        }
    }

    public Boolean[] compareAndSet(String schema, String tableName, List<Object[]> tuples, List<Object[]> expects) {
        long jobSeqId = tso();
        ITransaction transaction = getTransaction(schema, tableName);
        Job job;
        if (transaction != null) {
            job = jobManager.createJob(jobSeqId, jobSeqId, transaction.getTxnId(), null);
        } else {
            job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        }
        IdGeneratorImpl idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        MetaService metaService = getSubMetaService(schema);
        Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        CommonId jobId = job.getJobId();
        List<Object[]> newTuples = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            Object[] tuple = tuples.get(i);
            Object[] expect = expects.get(i);
            Object[] newTuple = new Object[tuple.length + expect.length];
            System.arraycopy(tuple, 0, newTuple, 0, tuple.length);
            System.arraycopy(expect, 0, newTuple, tuple.length, expect.length);
            newTuples.add(newTuple);
        }
        try {
            // values --> partition --> compareAndSet --> root
            Location currentLocation = MetaService.root().currentLocation();
            List<Vertex> valuesOutputs = values(
                job,
                idGenerator,
                currentLocation,
                newTuples);
            List<Vertex> partitionOutputs = copy(
                table,
                job,
                idGenerator,
                currentLocation,
                transaction,
                valuesOutputs);
            List<Vertex> compareAndSetOutputs = compareAndSet(
                table,
                idGenerator,
                currentLocation,
                partitionOutputs);
            List<Vertex> root = root(
                job,
                idGenerator,
                currentLocation,
                compareAndSetOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            Boolean[] keyState = null;
            while (iterator.hasNext()) {
                Boolean[] state = (Boolean[]) iterator.next()[1];
                if (keyState != null) {
                    Boolean[] dest = new Boolean[keyState.length + state.length];
                    System.arraycopy(keyState, 0, dest, 0, keyState.length);
                    System.arraycopy(state, 0, dest, keyState.length, state.length);
                    keyState = dest;
                }
                keyState = state;
            }
            return keyState;
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    public Iterator<Record> scan(String schema, String tableName, OpKeyRange keyRange) {
        long jobSeqId = tso();
        Job job = jobManager.createJob(jobSeqId, jobSeqId, CommonId.EMPTY_TRANSACTION, null);
        IdGenerator idGenerator = new IdGeneratorImpl(job.getJobId().seq);

        CommonId jobId = job.getJobId();
        try {
            MetaService metaService = getSubMetaService(schema);
            Table table = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
            Location currentLocation = MetaService.root().currentLocation();
            // scan --> coalesce --> root
            List<Vertex> scanOutputs = scan(schema, tableName, job, idGenerator, currentLocation, keyRange);
            List<Vertex> coalesceOutputs = coalesce(idGenerator, scanOutputs);
            List<Vertex> root = root(job, idGenerator, currentLocation, coalesceOutputs);
            if (root.size() > 0) {
                throw new IllegalStateException("There root of plan must be `DingoRoot`");
            }

            Iterator<Object[]> iterator = jobManager.createIterator(job, null);
            List<Record> records = new ArrayList<>();
            while (iterator.hasNext()) {
                Object[] next = iterator.next();
                records.add(new Record(next, table.getColumns()));
            }
            return records.iterator();
        } finally {
            jobManager.removeJob(jobId);
        }
    }

    private List<Vertex> scan(String schema,
                              String tableName,
                              Job job,
                              IdGenerator idGenerator,
                              Location currentLocation,
                              OpKeyRange keyRange
    ) {
        MetaService metaService = getSubMetaService(schema);
        Table td = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        CommonId tableId = Parameters.nonNull(metaService.getTable(tableName).getTableId(), "Table not found.");
        io.dingodb.codec.KeyValueCodec codec = CodecService.INSTANCE.createKeyValueCodec(
            tableId, td.tupleType(), td.keyMapping()
        );

        Key start = keyRange.start;
        Object[] dst = new Object[td.getColumns().size()];
        byte[] startBytes = codec.encodeKey(mapKey2(start.getUserKey().toArray(), dst, td.getColumns(),
            start.columnOrder ? td.keyColumns() : OperationUtils.sortColumns(td.keyColumns())));
        Key end = keyRange.end;
        Object[] dst1 = new Object[td.getColumns().size()];
        byte[] endBytes = codec.encodeKey(mapKey2(end.getUserKey().toArray(), dst1, td.getColumns(),
            end.columnOrder ? td.keyColumns() : OperationUtils.sortColumns(td.keyColumns())));

        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            MetaService.root().getRangeDistribution(tableId),
            startBytes,
            endBytes,
            keyRange.withStart,
            keyRange.withEnd,
            null,
            false,
            false,
            null);
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionParam);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);

        List<Vertex> outputs = new ArrayList<>();
        for (int i = 0; i <= td.getPartitions().size(); i++) {
            task = job.getOrCreate(currentLocation, idGenerator);
            PartRangeScanParam param = new PartRangeScanParam(
                tableId,
                td.tupleType(),
                td.keyMapping(),
                null,
                null,
                null,
                null,
                td.tupleType(),
                true
            );
            Vertex scanVertex = new Vertex(PART_RANGE_SCAN, param);
            scanVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(calcVertex, scanVertex);
            calcVertex.addEdge(edge);
            scanVertex.addIn(edge);
            task.putVertex(scanVertex);
            outputs.add(scanVertex);
        }
        return outputs;
    }

    private List<Vertex> rangeDelete(String schema, String tableName,
                                    Job job, IdGenerator idGenerator,
                                    Location currentLocation,
                                    Key begin, Key end, boolean withBegin, boolean withEnd
    ) {
        MetaService metaService = getSubMetaService(schema);
        CommonId tableId = Parameters.nonNull(metaService.getTable(tableName).getTableId(), "Table not found.");
        Table td = Parameters.nonNull(metaService.getTable(tableName), "Table not found.");
        io.dingodb.codec.KeyValueCodec codec = CodecService.INSTANCE.createKeyValueCodec(
            tableId, td.tupleType(), td.keyMapping()
        );

        byte[] startKey = codec.encodeKeyPrefix(mapKeyPrefix(td, begin), begin.userKey.size());
        byte[] endKey = codec.encodeKeyPrefix(mapKeyPrefix(td, end), end.userKey.size());
        DistributionSourceParam distributionParam = new DistributionSourceParam(
            td,
            MetaService.root().getRangeDistribution(tableId),
            startKey,
            endKey,
            withBegin,
            withEnd,
            null,
            false,
            false,
            null);
        Vertex calcVertex = new Vertex(CALC_DISTRIBUTION, distributionParam);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        calcVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(calcVertex);

        List<Vertex> outputs = new ArrayList<>();
        for (int i = 0; i <= td.getPartitions().size(); i++) {
            PartRangeDeleteParam param = new PartRangeDeleteParam(tableId, td.tupleType(), td.keyMapping());
            Vertex deleteVertex = new Vertex(PART_RANGE_DELETE, param);
            task = job.getOrCreate(currentLocation, idGenerator);
            deleteVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge edge = new Edge(calcVertex, deleteVertex);
            calcVertex.addEdge(edge);
            deleteVertex.addIn(edge);
            task.putVertex(deleteVertex);
            OutputHint hint = new OutputHint();
            hint.setToSumUp(true);
            deleteVertex.setHint(hint);
            outputs.add(deleteVertex);
        }
        return outputs;
    }

    private List<Vertex> values(Job job,
                                IdGenerator idGenerator,
                                Location currentLocation,
                                List<Object[]> tuples
    ) {
        List<Vertex> outputs = new LinkedList<>();
        ValuesParam valuesParam = new ValuesParam(tuples, null);
        Vertex values = new Vertex(VALUES, valuesParam);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        task.setContext(Context.builder().pin(0).keyState(new ArrayList<>()).build());
        values.setId(idGenerator.getOperatorId(task.getId()));
        OutputHint hint = new OutputHint();
        hint.setLocation(currentLocation);
        values.setHint(hint);
        task.putVertex(values);

        outputs.add(values);
        return outputs;
    }

    private List<Vertex> getByKey(Table td,
                                  Job job,
                                  IdGenerator idGenerator,
                                  Location currentLocation,
                                  ITransaction transaction,
                                  List<Object[]> tuples
    ) {
        CommonId tableId = td.getTableId();
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
            metaService.getRangeDistribution(tableId);
        List<Vertex> outputs = new LinkedList<>();

        GetDistributionParam distributionParam = new GetDistributionParam(tuples, td.keyMapping(), td, parts);
        Vertex distributionVertex = new Vertex(GET_DISTRIBUTION, distributionParam);
        Task task = job.getOrCreate(currentLocation, idGenerator);
        distributionVertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(distributionVertex);

        Vertex vertex;
        long scanTs = Optional.ofNullable(transaction).map(ITransaction::getStartTs).orElse(0L);
        if (transaction != null) {
            vertex = new Vertex(TXN_GET_BY_KEYS,
                new TxnGetByKeysParam(tableId,
                    td.tupleType(),
                    td.keyMapping(),
                    null,
                    null,
                    td,
                    scanTs,
                    transaction.getIsolationLevel(),
                    transaction.getLockTimeOut(),
                    true));
        } else {
            vertex = new Vertex(GET_BY_KEYS,
                new GetByKeysParam(tableId, td.tupleType(), td.keyMapping(), null, null, td));
        }
        OutputHint hint = new OutputHint();
        vertex.setHint(hint);
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        Edge edge = new Edge(distributionVertex, vertex);
        distributionVertex.addEdge(edge);
        vertex.addIn(edge);
        task.putVertex(vertex);
        outputs.add(vertex);
        return outputs;
    }

    private List<Vertex> copy(Table td,
                              Job job,
                              IdGenerator idGenerator,
                              Location currentLocation,
                              ITransaction transaction,
                              List<Vertex> inputs
    ) {
        List<Vertex> outpus = new LinkedList<>();
        CommonId tableId = td.getTableId();
        for (Vertex input : inputs) {
            NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> parts =
                metaService.getRangeDistribution(tableId);
            Task task = input.getTask();
            Vertex copyVertex = new Vertex(COPY, new CopyParam());
            copyVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge inputEdge = new Edge(input, copyVertex);
            input.addEdge(inputEdge);
            copyVertex.addIn(inputEdge);
            task.putVertex(copyVertex);

            Vertex distributeVertex = new Vertex(DISTRIBUTE, new DistributionParam(tableId, td, parts));
            distributeVertex.setId(idGenerator.getOperatorId(task.getId()));
            Edge copyEdge = new Edge(copyVertex, distributeVertex);
            copyVertex.addEdge(copyEdge);
            distributeVertex.addIn(copyEdge);
            OutputHint hint = new OutputHint();
            hint.setLocation(currentLocation);
            distributeVertex.setHint(hint);
            task.putVertex(distributeVertex);
            outpus.add(distributeVertex);

            if (transaction != null) {
                for (IndexTable index : td.getIndexes()) {
                    parts = metaService.getRangeDistribution(index.tableId);
                    distributeVertex = new Vertex(DISTRIBUTE, new DistributionParam(index.tableId, td, parts, index));
                    distributeVertex.setId(idGenerator.getOperatorId(task.getId()));
                    copyEdge = new Edge(copyVertex, distributeVertex);
                    copyVertex.addEdge(copyEdge);
                    distributeVertex.addIn(copyEdge);
                    distributeVertex.setHint(hint);
                    task.putVertex(distributeVertex);
                    outpus.add(distributeVertex);
                }
            }
        }
        return outpus;
    }

    private List<Vertex> coalesce( IdGenerator idGenerator, List<Vertex> inputs) {
        Map<CommonId, List<Vertex>> inputsMap = new HashMap<>();
        for (Vertex input : inputs) {
            CommonId taskId = input.getTaskId();
            List<Vertex> list = inputsMap.computeIfAbsent(taskId, k -> new LinkedList<>());
            list.add(input);
        }
        List<Vertex> outputs = new LinkedList<>();
        for (Map.Entry<CommonId, List<Vertex>> entry : inputsMap.entrySet()) {
            List<Vertex> list = entry.getValue();
            int size = list.size();
            if (size <= 1) {
                // Need no coalescing.
                outputs.addAll(list);
            } else {
                Vertex one = list.get(0);
                Task task = one.getTask();
                CoalesceParam coalesceParam = new CoalesceParam(size);
                Vertex coalesce = new Vertex(COALESCE, coalesceParam);
                coalesce.setId(idGenerator.getOperatorId(task.getId()));
                task.putVertex(coalesce);
                int i = 0;
                for (Vertex input : list) {
                    input.addEdge(new Edge(input, coalesce));
                    input.setPin(i);
                    ++i;
                }
                coalesce.copyHint(one);
                Edge edge = new Edge(one, coalesce);
                coalesce.addIn(edge);
                if (one.isToSumUp()) {
                    SumUpParam sumUpParam = new SumUpParam();
                    Vertex sumUp = new Vertex(SUM_UP, sumUpParam);
                    sumUp.setId(idGenerator.getOperatorId(task.getId()));
                    task.putVertex(sumUp);
                    sumUp.copyHint(coalesce);
                    Edge sumUpEdge = new Edge(coalesce, sumUp);
                    coalesce.addEdge(sumUpEdge);
                    sumUp.addIn(sumUpEdge);
                    outputs.add(sumUp);
                } else {
                    outputs.add(coalesce);
                }
            }
        }
        return outputs;
    }

    private List<Vertex> delete(Table td,
                                IdGenerator idGenerator,
                                Location currentLocation,
                                ITransaction transaction,
                                List<Vertex> inputs
    ) {
        CommonId tableId = Parameters.nonNull(td.getTableId(), "Table not found.");
        List<Vertex> outputs = new LinkedList<>();

        for (Vertex input : inputs) {
            Task task = input.getTask();
            Vertex vertex;
            if (transaction != null) {
                boolean pessimistic = transaction.isPessimistic();
                vertex = new Vertex(TXN_PART_DELETE,
                    new TxnPartDeleteParam(
                        tableId,
                        td.tupleType(),
                        td.keyMapping(),
                        pessimistic,
                        transaction.getIsolationLevel(),
                        pessimistic ? transaction.getPrimaryKeyLock() : null,
                        transaction.getStartTs(),
                        pessimistic ? transaction.getForUpdateTs() : 0L,
                        transaction.getLockTimeOut(),
                        td
                    )
                );
            } else {
                vertex = new Vertex(PART_DELETE, new PartDeleteParam(tableId, td.tupleType(), td.keyMapping(), td));
            }
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            input.setPin(0);
            OutputHint hint = new OutputHint();
            hint.setToSumUp(true);
            vertex.setHint(hint);
            Edge edge = new Edge(input, vertex);
            input.addEdge(edge);
            vertex.addIn(edge);
            outputs.add(vertex);
        }
        return outputs;
    }

    private List<Vertex> insert(Table td,
                                IdGenerator idGenerator,
                                Location currentLocation,
                                ITransaction transaction,
                                List<Vertex> inputs
    ) {
        CommonId tableId = Parameters.nonNull(td.getTableId(), "Table not found.");
        List<Vertex> outputs = new LinkedList<>();

        for (Vertex input : inputs) {
            Task task = input.getTask();
            Vertex vertex;
            if (transaction != null) {
                boolean pessimistic = transaction.isPessimistic();
                vertex = new Vertex(TXN_PART_INSERT,
                    new TxnPartInsertParam(
                        tableId,
                        td.tupleType(),
                        td.keyMapping(),
                        pessimistic,
                        transaction.getIsolationLevel(),
                        pessimistic ? transaction.getPrimaryKeyLock() : null,
                        transaction.getStartTs(),
                        pessimistic ? transaction.getForUpdateTs() : 0L,
                        transaction.getLockTimeOut(),
                        td,
                        false,
                        0));

            } else {
                vertex = new Vertex(PART_INSERT, new PartInsertParam(tableId, td.tupleType(), td.keyMapping(), td, false, 0));
            }
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            input.setPin(0);
            OutputHint hint = new OutputHint();
            hint.setToSumUp(false);
            vertex.setHint(hint);
            Edge edge = new Edge(input, vertex);
            input.addEdge(edge);
            vertex.addIn(edge);
            outputs.add(vertex);
        }
        return outputs;
    }

    private List<Vertex> compareAndSet(Table td,
                                       IdGenerator idGenerator,
                                       Location currentLocation,
                                       List<Vertex> inputs
    ) {
        CommonId tableId = Parameters.nonNull(td.getTableId(), "Table not found.");

        List<Vertex> outputs = new LinkedList<>();

        for (Vertex input : inputs) {
            Task task = input.getTask();
            CompareAndSetParam param = new CompareAndSetParam(tableId, td.tupleType(), td.keyMapping(), td);
            Vertex vertex = new Vertex(COMPARE_AND_SET, param);
            vertex.setId(idGenerator.getOperatorId(task.getId()));
            task.putVertex(vertex);
            input.setPin(0);
            Edge edge = new Edge(input, vertex);
            input.addEdge(edge);
            vertex.addIn(edge);
            outputs.add(vertex);
        }
        return outputs;
    }

    private List<Vertex> root(Job job, IdGenerator idGenerator, Location currentLocation, List<Vertex> inputs) {
        if (inputs.size() != 1) {
            throw new IllegalStateException("There must be one input to job root");
        }
        Vertex input = sole(inputs);
        DingoType dingoType = new LongType(false);
        RootParam param = new RootParam(DingoTypeFactory.tuple(new DingoType[]{dingoType}), null);
        Vertex vertex = new Vertex(ROOT, param);
        Task task = input.getTask();
        CommonId id = idGenerator.getOperatorId(task.getId());
        vertex.setId(id);
        Edge edge = new Edge(input, vertex);
        input.addEdge(edge);
        vertex.addIn(edge);
        task.putVertex(vertex);
        task.markRoot(id);
        job.markRoot(task.getId());
        return ImmutableList.of();
    }

}
