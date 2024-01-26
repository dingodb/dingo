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

package io.dingodb.store.proxy.mapper;

import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.entity.IndexTable;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.MetricType;
import io.dingodb.sdk.service.entity.common.ScalarField;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;
import io.dingodb.sdk.service.entity.common.ScalarIndexParameter;
import io.dingodb.sdk.service.entity.common.ScalarIndexType;
import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.DiskannParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.FlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.HnswParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfFlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfPqParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexType;
import io.dingodb.sdk.service.entity.meta.TableDefinition;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.store.proxy.mapper.Mapper.JSON;

public interface IndexMapper {

    default void setIndex(IndexTable.IndexTableBuilder builder, IndexParameter indexParameter) {
        if (indexParameter.getIndexType() == IndexType.INDEX_TYPE_VECTOR) {
            switch (indexParameter.getVectorIndexParameter().getVectorIndexType()) {
                case VECTOR_INDEX_TYPE_FLAT:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_FLAT);
                    break;
                case VECTOR_INDEX_TYPE_IVF_FLAT:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_IVF_FLAT);
                    break;
                case VECTOR_INDEX_TYPE_IVF_PQ:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_IVF_PQ);
                    break;
                case VECTOR_INDEX_TYPE_HNSW:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_HNSW);
                    break;
                case VECTOR_INDEX_TYPE_DISKANN:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_DISKANN);
                    break;
                case VECTOR_INDEX_TYPE_BRUTEFORCE:
                    builder.indexType(io.dingodb.meta.entity.IndexType.VECTOR_BRUTEFORCE);
                    break;
                default:
                    throw new IllegalStateException(
                        "Unexpected value: " + indexParameter.getVectorIndexParameter().getVectorIndexType()
                    );
            }
        } else {
            builder.indexType(io.dingodb.meta.entity.IndexType.SCALAR);
        }
        builder.properties(toMap(
            Optional.mapOrNull(indexParameter.getVectorIndexParameter(), VectorIndexParameter::getVectorIndexParameter)
        ));
    }

    default Properties toMap(Object target) {
        if (target == null) {
            return new Properties();
        }
        return JSON.convertValue(target, Properties.class);
    }

    default void resetIndexParameter(TableDefinition indexDefinition) {
        Map<String, String> properties = indexDefinition.getProperties();
        String indexType = properties.get("indexType");
        if (indexType.equals("scalar")) {
            indexDefinition.setIndexParameter(
                IndexParameter.builder()
                    .indexType(IndexType.INDEX_TYPE_SCALAR)
                    .scalarIndexParameter(ScalarIndexParameter.builder()
                        .isUnique(false)
                        .scalarIndexType(ScalarIndexType.SCALAR_INDEX_TYPE_LSM)
                        .build()
                    ).build()
            );
        } else {
            VectorIndexParameter vectorIndexParameter;
            int dimension = Optional.mapOrThrow(
                properties.get("dimension"), Integer::parseInt, indexDefinition.getName() + " vector index dimension is null."
            );
            MetricType metricType;
            String metricType1 = properties.getOrDefault("metricType", "L2");
            switch (metricType1.toUpperCase()) {
                case "INNER_PRODUCT":
                    metricType = MetricType.METRIC_TYPE_INNER_PRODUCT;
                    break;
                case "COSINE":
                    metricType = MetricType.METRIC_TYPE_COSINE;
                    break;
                case "L2":
                    metricType = MetricType.METRIC_TYPE_L2;
                    break;
                default:
                    throw new IllegalStateException("Unsupported metric type: " + metricType1);
            }
            switch (properties.getOrDefault("type", "HNSW").toUpperCase()) {
                case "DISKANN":
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_DISKANN)
                        .vectorIndexParameter(
                            DiskannParameter.builder().dimension(dimension).metricType(metricType).build()
                        )
                        .build();
                    break;
                case "FLAT":
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_FLAT)
                        .vectorIndexParameter(
                            FlatParameter.builder().dimension(dimension).metricType(metricType).build()
                        ).build();
                    break;
                case "IVFPQ": {
                    int ncentroids = Integer.parseInt(properties.getOrDefault("ncentroids", "0"));
                    int nsubvector = Integer.parseInt(Parameters.nonNull(properties.get("nsubvector"), "nsubvector"));
                    int bucketInitSize = Integer.parseInt(properties.getOrDefault("bucketInitSize", "0"));
                    int bucketMaxSize = Integer.parseInt(properties.getOrDefault("bucketMaxSize", "0"));
                    int nbitsPerIdx = Integer.parseInt(properties.getOrDefault("nbitsPerIdx", "0"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_IVF_PQ)
                        .vectorIndexParameter(IvfPqParameter.builder()
                            .dimension(dimension)
                            .metricType(metricType)
                            .ncentroids(ncentroids)
                            .nsubvector(nsubvector)
                            .bucketInitSize(bucketInitSize)
                            .bucketMaxSize(bucketMaxSize)
                            .nbitsPerIdx(nbitsPerIdx)
                            .build()
                        ).build();
                    break;
                }
                case "IVFFLAT": {
                    int ncentroids = Integer.valueOf(properties.getOrDefault("ncentroids", "0"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_IVF_FLAT)
                        .vectorIndexParameter(
                            IvfFlatParameter.builder()
                                .dimension(dimension)
                                .metricType(metricType)
                                .ncentroids(ncentroids)
                                .build()
                        ).build();
                    break;
                }
                case "HNSW": {
                    int efConstruction = Integer.valueOf(properties.getOrDefault("efConstruction", "40"));
                    int nlinks = Integer.valueOf(properties.getOrDefault("nlinks", "32"));
                    vectorIndexParameter = VectorIndexParameter.builder()
                        .vectorIndexType(VectorIndexType.VECTOR_INDEX_TYPE_HNSW)
                        .vectorIndexParameter(HnswParameter.builder()
                            .dimension(dimension)
                            .metricType(metricType)
                            .efConstruction(efConstruction)
                            .maxElements(Integer.MAX_VALUE)
                            .nlinks(nlinks)
                            .build()
                        ).build();
                    break;
                }
                default:
                    throw new IllegalStateException("Unsupported type: " + properties.get("type"));
            }

            indexDefinition.setIndexParameter(
                IndexParameter.builder()
                    .indexType(IndexType.INDEX_TYPE_VECTOR)
                    .vectorIndexParameter(vectorIndexParameter)
                    .build()
            );
        }
    }

    default ScalarValue scalarValueTo(io.dingodb.store.api.transaction.data.ScalarValue scalarValue) {
        return ScalarValue.builder()
            .fieldType(fieldTypeTo(scalarValue.getFieldType()))
            .fields(scalarValue.getFields().stream().map(f -> scalarFieldTo(f, scalarValue.getFieldType())).collect(Collectors.toList()))
            .build();
    }

    default ScalarField scalarFieldTo(
        io.dingodb.store.api.transaction.data.ScalarField field,
        io.dingodb.store.api.transaction.data.ScalarValue.ScalarFieldType type
    ) {
        switch (type) {
            case BOOL:
                return ScalarField.builder().data(ScalarField.DataNest.BoolData.of((Boolean) field.getData())).build();
            case INTEGER:
                return ScalarField.builder().data(ScalarField.DataNest.IntData.of((Integer) field.getData())).build();
            case LONG:
                return ScalarField.builder().data(ScalarField.DataNest.LongData.of((Long) field.getData())).build();
            case FLOAT:
                return ScalarField.builder().data(ScalarField.DataNest.FloatData.of((Float) field.getData())).build();
            case DOUBLE:
                return ScalarField.builder().data(ScalarField.DataNest.DoubleData.of((Double) field.getData())).build();
            case STRING:
                return ScalarField.builder().data(ScalarField.DataNest.StringData.of((String) field.getData())).build();
            case BYTES:
                return ScalarField.builder().data(ScalarField.DataNest.BytesData.of((byte[]) field.getData())).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    default ScalarFieldType fieldTypeTo(io.dingodb.store.api.transaction.data.ScalarValue.ScalarFieldType fieldType) {
        switch (fieldType) {
            case NONE:
                return ScalarFieldType.NONE;
            case BOOL:
                return ScalarFieldType.BOOL;
            case INTEGER:
                return ScalarFieldType.INT32;
            case LONG:
                return ScalarFieldType.INT64;
            case FLOAT:
                return ScalarFieldType.FLOAT32;
            case DOUBLE:
                return ScalarFieldType.DOUBLE;
            case STRING:
                return ScalarFieldType.STRING;
            case BYTES:
                return ScalarFieldType.BYTES;
            default:
                return ScalarFieldType.UNRECOGNIZED;
        }
    }

}
