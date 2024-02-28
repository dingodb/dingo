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

package io.dingodb.client.vector;

import io.dingodb.client.VectorContext;
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorScanQuery;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.grpc.serializer.SizeUtils;
import io.dingodb.sdk.service.entity.common.Engine;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.ValueType;
import io.dingodb.sdk.service.entity.common.VectorIndexMetrics;
import io.dingodb.sdk.service.entity.common.VectorWithDistance;
import io.dingodb.sdk.service.entity.common.VectorWithId;
import io.dingodb.sdk.service.entity.meta.IndexDefinition;
import io.dingodb.sdk.service.entity.meta.IndexMetrics;
import io.dingodb.sdk.service.entity.meta.Partition;
import io.dingodb.sdk.service.entity.meta.PartitionRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.dingodb.client.vector.VectorKeyCodec.encode;

public class VectorClient {

    private final String schema;

    private IndexService indexService;

    public static final int MAX_MESSAGE_SIZE = 8 * 1024 * 1024;

    public Integer retryTimes = 20;

    public VectorClient(String coordinatorSvr) {
        this(coordinatorSvr, 30);
    }

    public VectorClient(String coordinatorSvr, Integer retryTimes) {
        this(coordinatorSvr, "DINGO", retryTimes);
    }

    public VectorClient(String coordinatorSvr, String schema, Integer retryTimes) {
        this.indexService = new IndexService(coordinatorSvr, retryTimes);
        this.schema = schema.toUpperCase();
        this.retryTimes = retryTimes;
    }

    public boolean createIndex(io.dingodb.client.common.IndexDefinition index) {
        return createIndex(schema, index);
    }

    public boolean createIndex(String schema, io.dingodb.client.common.IndexDefinition index) {
        List<PartitionDetailDefinition> details = index.getIndexPartition().getDetails();
        if (details == null || details.isEmpty()) {
            details = new ArrayList<>();
        } else {
            details = new ArrayList<>(details);
        }
        details.add(new PartitionDetailDefinition(null, null, new Object[] {0L}));
        List<Partition> partitions = details.stream()
            .mapToLong($ -> Long.parseLong($.getOperand()[0].toString()))
            .sorted()
            .mapToObj($ -> Partition.builder().range(Range.builder().startKey(encode($)).build()).build())
            .collect(Collectors.toList());
        IndexDefinition definition = IndexDefinition.builder()
            .name(index.getName())
            .withAutoIncrment(index.getIsAutoIncrement())
            .autoIncrement(index.getAutoIncrement())
            .engine(Engine.LSM)
            .indexParameter(index.getIndexParameter())
            .replica(index.getReplica())
            .version(index.getVersion())
            .indexPartition(PartitionRule.builder().partitions(partitions).build())
            .build();
        return indexService.createIndex(schema, definition);
    }

    public boolean updateMaxElements(String schema, String index, int maxElements) {
        return indexService.updateMaxElements(schema, index, maxElements);
    }

    public boolean dropIndex(String indexName) {
        return dropIndex(schema, indexName);
    }

    public boolean dropIndex(String schema, String indexName) {
        return indexService.dropIndex(schema, indexName);
    }

    public io.dingodb.client.common.IndexDefinition getIndex(String index) {
        return getIndex(schema, index);
    }

    public io.dingodb.client.common.IndexDefinition getIndex(String schema, String index) {
        IndexDefinition definition = indexService.getIndex(schema, index);
        return mapping(definition);
    }

    private io.dingodb.client.common.IndexDefinition mapping(IndexDefinition definition) {
        return io.dingodb.client.common.IndexDefinition.builder()
            .name(definition.getName())
            .isAutoIncrement(definition.isWithAutoIncrment())
            .autoIncrement(definition.getAutoIncrement())
            .indexParameter(definition.getIndexParameter())
            .replica(definition.getReplica())
            .version(definition.getVersion())
            .indexPartition(new PartitionDefinition(
                null, null, definition.getIndexPartition().getPartitions().stream()
                .map(Partition::getRange)
                .map(Range::getStartKey)
                .mapToLong(VectorKeyCodec::decode)
                .sorted()
                .skip(1)
                .mapToObj($ -> new Object[]{$})
                .map($ -> new PartitionDetailDefinition(null, null, $))
                .collect(Collectors.toList())
            )).build();
    }

    public List<io.dingodb.client.common.IndexDefinition> getIndexes(String schema) {
        return indexService.getIndexes(schema).stream().map(this::mapping).collect(Collectors.toList());
    }

    public IndexMetrics getIndexMetrics(String schema, String index) {
        return indexService.getIndexMetrics(schema, index);
    }

    public List<VectorWithId> vectorAdd(String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors);
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors, false, false);
    }

    public List<VectorWithId> vectorAdd(String schema, String indexName, List<VectorWithId> vectors,
                                        Boolean replaceDeleted, Boolean isUpdate) {
        VectorContext context = VectorContext.builder().replaceDeleted(replaceDeleted).isUpdate(isUpdate).build();
        int dimension = indexService.getIndex(schema, indexName)
            .getIndexParameter().getVectorIndexParameter().getVectorIndexParameter().getDimension();
        long count = checkDimension(vectors, dimension);
        checkAddSize(vectors);

        if (dimension != 0 && count > 0) {
            throw new DingoClientException("Dimension is not the same length as its value or from the time it was created");
        }
        return indexService.exec(schema, indexName, VectorAddOperation.getInstance(), vectors, context);
    }

    public List<VectorWithId> vectorUpsert(String schema, String indexName, List<VectorWithId> vectors) {
        return vectorAdd(schema, indexName, vectors, false, true);
    }

    private void checkAddSize(List<VectorWithId> vectors) {
        if (vectors.size() > 1024) {
            throw new DingoClientException("Param vectors size " + vectors.size() + " is exceed max batch count 1024");
        }
        int size = SizeUtils.sizeOf(1, vectors, SizeUtils::sizeOf);
        if (size > MAX_MESSAGE_SIZE) {
            throw new DingoClientException("Message exceeds maximum size " + MAX_MESSAGE_SIZE + " : " + size);
        }
    }

    private static long checkDimension(List<VectorWithId> vectors, int dimension) {
        return vectors.stream()
            .map(VectorWithId::getVector)
            .filter(v -> v.getDimension() != dimension || (v.getValueType() == ValueType.FLOAT
                ? v.getFloatValues().size() != dimension : v.getBinaryValues().size() != dimension))
            .count();
    }

    public List<VectorDistanceArray> vectorSearch(String indexName, VectorSearch vectorSearch) {
        return vectorSearch(schema, indexName, vectorSearch);
    }

    public List<VectorDistanceArray> vectorSearch(String schema, String indexName, VectorSearch vectorSearch) {
        List<VectorDistanceArray> distanceArrays = indexService.exec(
            schema,
            indexName,
            VectorSearchOperation.getInstance(),
            vectorSearch);

        List<VectorDistanceArray> result = new ArrayList<>();
        for (VectorDistanceArray distanceArray : distanceArrays) {
            if (distanceArray == null) {
                continue;
            }
            List<VectorWithDistance> withDistances = distanceArray.getVectorWithDistances();
            int topN = vectorSearch.getParameter().getTopN();
            if (withDistances.size() <= topN) {
                result.add(distanceArray);
                continue;
            }
            result.add(new VectorDistanceArray(withDistances.subList(0, topN)));
        }
        return result;
    }

    public Map<Long, VectorWithId> vectorBatchQuery(String schema, String indexName, Set<Long> ids,
                                                    boolean withoutVectorData,
                                                    boolean withoutScalarData,
                                                    List<String> selectedKeys) {
        VectorContext vectorContext = VectorContext.builder()
            .withoutVectorData(withoutVectorData)
            .withoutScalarData(withoutScalarData)
            .selectedKeys(selectedKeys)
            .build();
        return indexService.exec(schema, indexName, VectorBatchQueryOperation.getInstance(), ids, vectorContext);
    }

    /**
     * Get Boundary by schema and index name.
     * @param schema schema name, default is 'dingo'
     * @param indexName index name
     * @param isGetMin if true, get min id, else get max id
     * @return id
     */
    public Long vectorGetBorderId(String schema, String indexName, Boolean isGetMin) {
        long[] longArr = indexService.exec(schema, indexName, VectorGetIdOperation.getInstance(), isGetMin);
        return (isGetMin ? Arrays.stream(longArr).min() : Arrays.stream(longArr).max()).getAsLong();
    }

    public List<VectorWithId> vectorScanQuery(String schema, String indexName, VectorScanQuery query) {
        List<VectorWithId> result = indexService.exec(schema, indexName, VectorScanQueryOperation.getInstance(), query);
        return query.getMaxScanCount() > result.size()
            ? result : result.subList(0, Math.toIntExact(query.getMaxScanCount()));
    }

    public VectorIndexMetrics getRegionMetrics(String schema, String indexName) {
        return indexService.exec(schema, indexName, VectorGetRegionMetricsOperation.getInstance(), null);
    }

    public List<Boolean> vectorDelete(String indexName, List<Long> ids) {
        return vectorDelete(schema, indexName, ids);
    }

    public List<Boolean> vectorDelete(String schema, String indexName, List<Long> ids) {
        return indexService.exec(schema, indexName, VectorDeleteOperation.getInstance(), ids);
    }

    public Long vectorCount(String schema, String indexName) {
        return indexService.exec(schema, indexName, VectorCountOperation.getInstance(), null);
    }


}
