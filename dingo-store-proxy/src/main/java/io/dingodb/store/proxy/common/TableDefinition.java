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

package io.dingodb.store.proxy.common;

import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.sdk.common.index.DiskAnnParam;
import io.dingodb.sdk.common.index.FlatParam;
import io.dingodb.sdk.common.index.HnswParam;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.IvfFlatParam;
import io.dingodb.sdk.common.index.IvfPqParam;
import io.dingodb.sdk.common.index.ScalarIndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class TableDefinition implements Table {

    private final io.dingodb.common.table.TableDefinition tableDefinition;

    @Setter
    private Map<String, String> properties;

    @Setter
    private String name;

    public TableDefinition(io.dingodb.common.table.TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
    }

    @Override
    public String getName() {
        if (name != null) {
            return name;
        }
        return tableDefinition.getName();
    }

    @Override
    public List<Column> getColumns() {
        return tableDefinition.getColumns().stream().map(ColumnDefinition::new).collect(Collectors.toList());
    }

    @Override
    public int getVersion() {
        return tableDefinition.getVersion();
    }

    @Override
    public int getTtl() {
        return tableDefinition.getTtl();
    }

    @Override
    public Partition getPartition() {
        if (tableDefinition.getPartDefinition() == null) {
            return null;
        }
        return new PartitionRule(tableDefinition.getPartDefinition());
    }

    @Override
    public String getEngine() {
        return Optional.ofNullable(tableDefinition.getEngine()).orElse("ENG_ROCKSDB");
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> map = new HashMap<>();
        if (tableDefinition.getProperties() != null) {
            tableDefinition.getProperties().forEach((key, value) -> map.put((String) key, (String) value));
        }

        return map;
    }

    @Override
    public long getAutoIncrement() {
        return tableDefinition.getAutoIncrement();
    }

    @Override
    public String getCreateSql() {
        return tableDefinition.getCreateSql();
    }

    @Override
    public int getReplica() {
        return tableDefinition.getReplica();
    }

    @Override
    public IndexParameter getIndexParameter() {
        if (properties == null) {
            return null;
        }

        String indexType = properties.get("indexType");
        if (indexType.equals("scalar")) {
            return new IndexParameter(
                IndexParameter.IndexType.INDEX_TYPE_SCALAR,
                new ScalarIndexParameter(ScalarIndexParameter.ScalarIndexType.SCALAR_INDEX_TYPE_LSM, false)
            );
        } else {
            VectorIndexParameter vectorIndexParameter;
            int dimension = Optional.mapOrThrow(properties.get("dimension"), Integer::parseInt,
                tableDefinition.getName() + " vector index dimension is null.");
            VectorIndexParameter.MetricType metricType = getMetricType(properties.getOrDefault("metricType", "L2"));
            switch (properties.getOrDefault("type", "HNSW").toUpperCase()) {
                case "DISKANN":
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_DISKANN,
                        new DiskAnnParam(dimension, metricType)
                    );
                    break;
                case "FLAT":
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_FLAT,
                        new FlatParam(dimension, metricType)
                    );
                    break;
                case "IVFPQ":
                    ;
                    int ncentroids = Integer.parseInt(properties.getOrDefault("ncentroids", "0"));
                    int nsubvector = Integer.parseInt(Parameters.nonNull(properties.get("nsubvector"), "nsubvector"));
                    int bucketInitSize = Integer.parseInt(properties.getOrDefault("bucketInitSize", "0"));
                    int bucketMaxSize = Integer.parseInt(properties.getOrDefault("bucketMaxSize", "0"));
                    int nbitsPerIdx = Integer.parseInt(properties.getOrDefault("nbitsPerIdx", "0"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_PQ,
                        new IvfPqParam(
                            dimension, metricType, ncentroids, nsubvector, bucketInitSize, bucketMaxSize, nbitsPerIdx
                        )
                    );
                    break;
                case "IVFFLAT": {
                    int ncentroids2 = Integer.valueOf(properties.getOrDefault("ncentroids", "0"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_FLAT,
                        new IvfFlatParam(dimension, metricType, ncentroids2)
                    );
                    break;
                }
                case "HNSW":
                    int efConstruction = Integer.valueOf(properties.getOrDefault("efConstruction", "40"));
                    int nlinks = Integer.valueOf(properties.getOrDefault("nlinks", "32"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_HNSW,
                        new HnswParam(dimension, metricType, efConstruction, Integer.MAX_VALUE, nlinks)
                    );
                    break;
                default:
                    throw new IllegalStateException("Unsupported type: " + properties.get("type"));
            }

            return new IndexParameter(IndexParameter.IndexType.INDEX_TYPE_VECTOR, vectorIndexParameter);
        }
    }

    @Override
    public String getComment() {
        return tableDefinition.getComment();
    }

    @Override
    public String getCharset() {
        return tableDefinition.getCharset();
    }

    @Override
    public String getCollate() {
        return tableDefinition.getCollate();
    }

    @Override
    public String getTableType() {
        return tableDefinition.getTableType();
    }

    @Override
    public String getRowFormat() {
        return tableDefinition.getRowFormat();
    }

    @Override
    public long getCreateTime() {
        return tableDefinition.getCreateTime();
    }

    @Override
    public long getUpdateTime() {
        return tableDefinition.getUpdateTime();
    }

    private VectorIndexParameter.MetricType getMetricType(String metricType) {
        switch (metricType.toUpperCase()) {
            case "INNER_PRODUCT":
                return VectorIndexParameter.MetricType.METRIC_TYPE_INNER_PRODUCT;
            case "COSINE":
                return VectorIndexParameter.MetricType.METRIC_TYPE_COSINE;
            case "L2":
                return VectorIndexParameter.MetricType.METRIC_TYPE_L2;
            default:
                throw new IllegalStateException("Unsupported metric type: " + metricType);
        }
    }
}
