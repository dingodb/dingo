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

package io.dingodb.store.common;

import io.dingodb.common.util.Optional;
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
            switch (properties.get("type").toUpperCase()) {
                case "HNSW":
                    int efConstruction = Integer.valueOf(properties.getOrDefault("efConstruction", "10"));
                    int nlinks = Integer.valueOf(properties.getOrDefault("nlinks", "10"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_HNSW,
                        new HnswParam(dimension, metricType, efConstruction, Integer.MAX_VALUE, nlinks)
                    );
                    break;
                case "DISKANN":
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_DISKANN,
                        new DiskAnnParam(dimension, metricType)
                    );
                    break;
                case "IVFPQ":
                    int ncentroids = Integer.valueOf(properties.getOrDefault("ncentroids", "10"));
                    int nsubvector = Integer.valueOf(properties.getOrDefault("nsubvector", "10"));
                    int bucketInitSize = Integer.valueOf(properties.getOrDefault("bucketInitSize", "10"));
                    int bucketMaxSize = Integer.valueOf(properties.getOrDefault("bucketMaxSize", "10"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_PQ,
                        new IvfPqParam(dimension, metricType, ncentroids, nsubvector, bucketInitSize, bucketMaxSize)
                    );
                    break;
                case "IVFFLAT":
                    int nsubvector2 = Integer.valueOf(properties.getOrDefault("ncentroids", "10"));
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_FLAT,
                        new IvfFlatParam(dimension, metricType, nsubvector2)
                    );
                    break;
                case "FLAT":
                default:
                    vectorIndexParameter = new VectorIndexParameter(
                        VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_FLAT,
                        new FlatParam(dimension, metricType)
                    );
            }

            return new IndexParameter(IndexParameter.IndexType.INDEX_TYPE_VECTOR, vectorIndexParameter);
        }
    }

    private VectorIndexParameter.MetricType getMetricType(String metricType) {
        switch (metricType.toUpperCase()) {
            case "INNER_PRODUCT":
                return VectorIndexParameter.MetricType.METRIC_TYPE_INNER_PRODUCT;
            case "COSINE":
                return VectorIndexParameter.MetricType.METRIC_TYPE_COSINE;
            case "L2":
            default:
                return VectorIndexParameter.MetricType.METRIC_TYPE_L2;
        }
    }
}
