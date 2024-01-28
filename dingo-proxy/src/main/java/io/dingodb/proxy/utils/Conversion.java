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

package io.dingodb.proxy.utils;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.dingodb.client.common.IndexDefinition;
import io.dingodb.client.common.VectorCoprocessor;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.proxy.bean.ClientBean;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.mapper.EntityMapper;
import io.dingodb.proxy.meta.ProxyMeta;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.service.entity.common.IndexParameter;
import io.dingodb.sdk.service.entity.common.IndexType;
import io.dingodb.sdk.service.entity.common.ScalarField;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.BoolData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.BytesData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.DoubleData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.FloatData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.IntData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.LongData;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.StringData;
import io.dingodb.sdk.service.entity.common.ScalarFieldType;
import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest;
import io.dingodb.sdk.service.entity.common.VectorIndexType;
import io.dingodb.sdk.service.entity.common.VectorScalardata;
import io.dingodb.sdk.service.entity.common.VectorWithId;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class Conversion {

    public static final EntityMapper MAPPER = ClientBean.mapper();

    public static IndexDefinition mapping(ProxyMeta.IndexDefinition definition) {
        return new IndexDefinition(
            definition.getName(),
            definition.getVersion(),
            mapping(definition.getIndexPartition()),
            definition.getReplica(),
            mapping(definition.getIndexParameter()),
            definition.getWithAutoIncrement(),
            definition.getAutoIncrement()
        );
    }

    public static ProxyMeta.IndexDefinition mapping(IndexDefinition index) {
        return ProxyMeta.IndexDefinition.newBuilder()
            .setName(index.getName())
            .setVersion(index.getVersion())
            .setIndexPartition(mapping(index.getIndexPartition()))
            .setReplica(index.getReplica())
            .setIndexParameter(mapping(index.getIndexParameter()))
            .setWithAutoIncrement(index.getIsAutoIncrement())
            .setAutoIncrement(index.getAutoIncrement())
            .build();
    }

    public static PartitionDefinition mapping(ProxyMeta.PartitionRule partitionRule) {
        return new PartitionDefinition(
            partitionRule.getFuncName(),
            partitionRule.getColumnsList(),
            partitionRule.getDetailsList().stream()
                .map(d -> new PartitionDetailDefinition(d.getPartName(), d.getOperator(), d.getOperandList().toArray()))
                .collect(Collectors.toList())
            );
    }

    public static ProxyMeta.PartitionRule mapping(PartitionDefinition partition) {
        return ProxyMeta.PartitionRule.newBuilder()
            .addAllDetails(partition.getDetails().stream()
                .map(d -> ProxyMeta.PartitionDetailDefinition.newBuilder()
                    .addAllOperand(Arrays.stream(d.getOperand())
                        .map(String::valueOf)
                        .collect(Collectors.toList()))
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    public static IndexParameter mapping(ProxyCommon.IndexParameter parameter) {
        ProxyCommon.VectorIndexParameter vectorIndexParameter = parameter.getVectorIndexParameter();
        VectorIndexType vectorIndexType = VectorIndexType.valueOf(vectorIndexParameter.getVectorIndexType().name());
        VectorIndexParameter vectorParameter;
        switch (vectorIndexParameter.getVectorIndexType()) {
            case VECTOR_INDEX_TYPE_FLAT:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getFlatParameter()))
                    .build();
                break;
            case VECTOR_INDEX_TYPE_IVF_FLAT:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getIvfFlatParameter()))
                    .build();
                break;
            case VECTOR_INDEX_TYPE_IVF_PQ:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getIvfPqParameter()))
                    .build();
                break;
            case VECTOR_INDEX_TYPE_HNSW:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getHnswParameter()))
                    .build();
                break;
            case VECTOR_INDEX_TYPE_DISKANN:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getDiskannParameter()))
                    .build();
                break;
            case VECTOR_INDEX_TYPE_BRUTEFORCE:
                vectorParameter = VectorIndexParameter.builder()
                    .vectorIndexType(vectorIndexType)
                    .vectorIndexParameter(MAPPER.mapping(vectorIndexParameter.getBruteforceParameter()))
                    .build();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + vectorIndexParameter.getVectorIndexType());
        }
        return IndexParameter.builder()
            .indexType(IndexType.INDEX_TYPE_VECTOR)
            .vectorIndexParameter(vectorParameter)
            .build();
    }

    private static <P extends VectorIndexParameterNest> P mapping(VectorIndexParameterNest parameter) {
        return (P) parameter;
    }

    public static ProxyCommon.IndexParameter mapping(IndexParameter parameter) {
        ProxyCommon.IndexParameter.Builder builder = ProxyCommon.IndexParameter.newBuilder()
            .setIndexType(ProxyCommon.IndexType.valueOf(parameter.getIndexType().name()));
        VectorIndexParameter vectorParameter = parameter.getVectorIndexParameter();
        ProxyCommon.VectorIndexParameter.Builder vectorBuilder = ProxyCommon.VectorIndexParameter.newBuilder()
            .setVectorIndexType(ProxyCommon.VectorIndexType.valueOf(vectorParameter.getVectorIndexType().name()));
        switch (vectorParameter.getVectorIndexType()) {
            case VECTOR_INDEX_TYPE_FLAT: {
                ProxyCommon.CreateFlatParam.Builder paramBuilder = ProxyCommon.CreateFlatParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setFlatParameter(paramBuilder.build());
                break;
            }
            case VECTOR_INDEX_TYPE_IVF_FLAT:{
                ProxyCommon.CreateIvfFlatParam.Builder paramBuilder = ProxyCommon.CreateIvfFlatParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setIvfFlatParameter(paramBuilder.build());
                break;
            }
            case VECTOR_INDEX_TYPE_IVF_PQ: {
                ProxyCommon.CreateIvfPqParam.Builder paramBuilder = ProxyCommon.CreateIvfPqParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setIvfPqParameter(paramBuilder.build());
                break;
            }
            case VECTOR_INDEX_TYPE_HNSW:{
                ProxyCommon.CreateHnswParam.Builder paramBuilder = ProxyCommon.CreateHnswParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setHnswParameter(paramBuilder.build());
                break;
            }
            case VECTOR_INDEX_TYPE_DISKANN:{
                ProxyCommon.CreateDiskAnnParam.Builder paramBuilder = ProxyCommon.CreateDiskAnnParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setDiskannParameter(paramBuilder.build());
                break;
            }
            case VECTOR_INDEX_TYPE_BRUTEFORCE:{
                ProxyCommon.CreateBruteForceParam.Builder paramBuilder = ProxyCommon.CreateBruteForceParam.newBuilder();
                MAPPER.mapping(mapping(vectorParameter.getVectorIndexParameter()), paramBuilder);
                vectorBuilder.setBruteforceParameter(paramBuilder.build());
                break;
            }
        }
        builder.setVectorIndexParameter(vectorBuilder.build());
        return builder.build();
    }

    public static VectorCoprocessor mapping(ProxyCommon.VectorCoprocessor coprocessor) {
        return VectorCoprocessor.builder()
            .schemaVersion(coprocessor.getSchemaVersion())
            .originalSchema(VectorCoprocessor.VectorSchemaWrapper.builder()
                .schemas(coprocessor.getOriginalSchema().getSchemaList().stream().map(Conversion::mapping).collect(Collectors.toList()))
                .commonId(coprocessor.getOriginalSchema().getCommonId())
                .build())
            .selection(coprocessor.getSelectionColumnsList())
            .expression(coprocessor.getExpression().toByteArray())
            .build();
    }

    public static VectorCoprocessor.ColumnDefinition mapping(ProxyCommon.ColumnDefinition definition) {
        return VectorCoprocessor.ColumnDefinition.builder()
            .name(definition.getName())
            .type(definition.getSqlType())
            .elementType(definition.getElementType())
            .primary(definition.getIndexOfKey())
            .nullable(definition.getNullable())
            .build();
    }

    public static VectorWithId mapping(ProxyCommon.VectorWithId withId) {
        ProxyCommon.Vector vector = withId.getVector();

        return io.dingodb.sdk.service.entity.common.VectorWithId.builder()
            .id(withId.getId())
            .vector(MAPPER.mapping(withId.getVector()))
            .scalarData(VectorScalardata.builder().scalarData(withId.getScalarDataMap().entrySet().stream().collect(
                Maps::newHashMap,
                (map, entry) -> map.put(entry.getKey(), mapping(entry.getValue())),
                Map::putAll
            )).build())
            .build();
    }

    public static ScalarValue mapping(ProxyCommon.ScalarValue value) {
        return ScalarValue.builder()
            .fieldType(ScalarFieldType.valueOf(value.getFieldType().name()))
            .fields(value.getFieldsList().stream()
                .map(f -> mapping(f, value.getFieldType()))
                .collect(Collectors.toList()))
            .build();
    }

    public static ScalarField mapping(ProxyCommon.ScalarField field, ProxyCommon.ScalarFieldType type) {
        switch (type) {
            case BOOL:
                return ScalarField.builder().data(BoolData.of(field.getBoolData())).build();
            case INT8:
            case INT16:
            case INT32:
                return ScalarField.builder().data(IntData.of(field.getIntData())).build();
            case INT64:
                return ScalarField.builder().data(LongData.of(field.getLongData())).build();
            case FLOAT32:
                return ScalarField.builder().data(FloatData.of(field.getFloatData())).build();
            case DOUBLE:
                return ScalarField.builder().data(DoubleData.of(field.getDoubleData())).build();
            case STRING:
                return ScalarField.builder().data(StringData.of(field.getStringData())).build();
//            case BYTES:
//                return ScalarField.builder().data(BytesData.of(field.getBytesData().toByteArray())).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static ProxyCommon.VectorWithId mapping(VectorWithId withId) {
        ProxyCommon.VectorWithId.Builder builder = ProxyCommon.VectorWithId.newBuilder();
        if (withId == null) {
            return builder.build();
        }
        if (withId.getVector() != null) {
            builder.setVector(MAPPER.mapping(withId.getVector()));
        } else {
            builder.setVector(ProxyCommon.Vector.newBuilder().build());
        }
        if (withId.getScalarData() != null) {
            builder.putAllScalarData(mapping(withId.getScalarData()));
        }
        builder.setId(withId.getId());
        return builder.build();
    }

    public static Map<String, ProxyCommon.ScalarValue> mapping(VectorScalardata scalarData) {
        if (scalarData == null) {
            return Collections.emptyMap();
        }
        return scalarData.getScalarData().entrySet().stream()
                .collect(Maps::newHashMap,
                    (map, entry) -> map.put(entry.getKey(), mapping(entry.getValue())),
                    Map::putAll);
    }

    public static ProxyCommon.Vector mapping(Vector vector) {
        return ProxyCommon.Vector.newBuilder()
            .setDimension(vector.getDimension())
            .setValueType(ProxyCommon.ValueType.valueOf(vector.getValueType().name()))
            .addAllFloatValues(vector.getFloatValues())
            .addAllBinaryValues(vector.getBinaryValues()
                .stream()
                .map(ByteString::copyFrom)
                .collect(Collectors.toList()))
            .build();
    }

    public static Vector mapping(ProxyCommon.Vector vector) {
        return new Vector(
            vector.getDimension(),
            Vector.ValueType.valueOf(vector.getValueType().name()),
            vector.getFloatValuesList(),
            vector.getBinaryValuesList().stream().map(ByteString::toByteArray).collect(Collectors.toList()));
    }

    public static ProxyCommon.ScalarValue mapping(ScalarValue value) {
        return ProxyCommon.ScalarValue.newBuilder()
            .setFieldType(ProxyCommon.ScalarFieldType.valueOf(value.getFieldType().name()))
            .addAllFields(value.getFields().stream().map(f -> mapping(f, value.getFieldType()))
                .collect(Collectors.toList()))
            .build();
    }

    public static ProxyCommon.ScalarField mapping(ScalarField field, ScalarFieldType type) {
        switch (type) {
            case BOOL:
                return ProxyCommon.ScalarField.newBuilder()
                    .setBoolData(((BoolData) field.getData()).isValue()).build();
            case INT8:
            case INT16:
            case INT32:
                return ProxyCommon.ScalarField.newBuilder()
                    .setIntData(((IntData) field.getData()).getValue()).build();
            case INT64:
                return ProxyCommon.ScalarField.newBuilder()
                    .setLongData(((LongData) field.getData()).getValue()).build();
            case FLOAT32:
                return ProxyCommon.ScalarField.newBuilder()
                    .setFloatData(((FloatData) field.getData()).getValue()).build();
            case DOUBLE:
                return ProxyCommon.ScalarField.newBuilder()
                    .setDoubleData(((DoubleData) field.getData()).getValue()).build();
            case STRING:
                return ProxyCommon.ScalarField.newBuilder()
                    .setStringData(((StringData) field.getData()).getValue()).build();
            case BYTES:
                return ProxyCommon.ScalarField.newBuilder().setBytesData(
                    ByteString.copyFrom(((BytesData) field.getData()).getValue())
                ).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }
}
