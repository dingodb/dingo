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

package io.dingodb.proxy.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.dingodb.client.common.VectorDistanceArray;
import io.dingodb.client.common.VectorSearch;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.expr.coding.CodingFlag;
import io.dingodb.expr.coding.RelOpCoder;
import io.dingodb.expr.rel.RelConfig;
import io.dingodb.expr.rel.RelOp;
import io.dingodb.expr.rel.TupleCompileContextImpl;
import io.dingodb.expr.rel.op.RelOpBuilder;
import io.dingodb.expr.runtime.type.TupleType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import io.dingodb.proxy.common.ProxyCommon;
import io.dingodb.proxy.common.ProxyCommon.CreateBruteForceParam;
import io.dingodb.proxy.common.ProxyCommon.CreateDiskAnnParam;
import io.dingodb.proxy.common.ProxyCommon.CreateFlatParam;
import io.dingodb.proxy.common.ProxyCommon.CreateHnswParam;
import io.dingodb.proxy.common.ProxyCommon.CreateIvfFlatParam;
import io.dingodb.proxy.common.ProxyCommon.CreateIvfPqParam;
import io.dingodb.proxy.config.JacksonConfig;
import io.dingodb.proxy.expr.langchain.Expr;
import io.dingodb.proxy.model.dto.VectorWithId;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.vector.Search;
import io.dingodb.sdk.common.vector.SearchDiskAnnParam;
import io.dingodb.sdk.common.vector.SearchFlatParam;
import io.dingodb.sdk.common.vector.SearchHnswParam;
import io.dingodb.sdk.common.vector.SearchIvfFlatParam;
import io.dingodb.sdk.common.vector.SearchIvfPqParam;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.service.entity.common.CoprocessorV2;
import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.Schema;
import io.dingodb.sdk.service.entity.common.SchemaWrapper;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.BruteforceParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.DiskannParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.FlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.HnswParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfFlatParameter;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter.VectorIndexParameterNest.IvfPqParameter;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest.Diskann;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest.Flat;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest.Hnsw;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest.IvfFlat;
import io.dingodb.sdk.service.entity.common.VectorSearchParameter.SearchNest.IvfPq;
import io.dingodb.sdk.service.entity.meta.IndexDefinition;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;
import org.mapstruct.Mappings;
import org.mapstruct.NullValuePropertyMappingStrategy;
import org.mapstruct.ReportingPolicy;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;


@Mapper(
    unmappedSourcePolicy = ReportingPolicy.IGNORE,
    unmappedTargetPolicy = ReportingPolicy.IGNORE,
    nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE
)
public interface EntityMapper {

    Map<String, ScalarValue> emptyScalarData = Collections.emptyMap();

    @Mapping(source = "partDefinition", target = "partition")
    TableDefinition mapping(io.dingodb.common.table.TableDefinition definition);

    @Mapping(source = "typeName", target = "type")
    ColumnDefinition mapping(io.dingodb.common.table.ColumnDefinition definition);

    IndexDefinition mapping(io.dingodb.client.common.IndexDefinition definition);

    default PartitionRule mapping(PartitionDefinition definition) {
        return new PartitionRule(
            definition.getFuncName(),
            definition.getColumns(),
            definition.getDetails().stream().map(this::mapping).collect(Collectors.toList()));
    }

    default PartitionDetailDefinition mapping(io.dingodb.common.partition.PartitionDetailDefinition definition) {
        return new PartitionDetailDefinition(definition.getPartName(), definition.getOperator(), definition.getOperand());
    }

    @Mapping(source = "scalarData", target = "scalarData.scalarData")
    io.dingodb.sdk.service.entity.common.VectorWithId mapping(VectorWithId withId);

    @Mapping(source = "scalarData.scalarData", target = "scalarData", defaultExpression = "java(emptyScalarData)")
    VectorWithId mapping(io.dingodb.sdk.service.entity.common.VectorWithId withId);

    List<io.dingodb.sdk.service.entity.common.VectorWithId> mapping(List<VectorWithId> ids);

    VectorSearch mapping(io.dingodb.proxy.model.dto.VectorSearch search);

    default CoprocessorV2 mapping(String langchainExpr) throws JsonProcessingException {
        if (langchainExpr == null || langchainExpr.isEmpty()) {
            return null;
        }
        return mapping(JacksonConfig.jsonMapper.readValue(langchainExpr, Expr.class));
    }

    default CoprocessorV2 mapping(Expr expr) {
        if (expr == null) {
            return null;
        }
        List<String> attrNames = new ArrayList<>();
        List<Schema> attrSchemas = new ArrayList<>();
        List<Type> attrTypes = new ArrayList<>();
        io.dingodb.expr.runtime.expr.Expr dingoExpr = expr.toDingoExpr(attrNames, attrSchemas, attrTypes);
        RelOp relOp = RelOpBuilder.builder().filter(dingoExpr).build();
        TupleType tupleType = Types.tuple(attrTypes.toArray(new Type[0]));
        relOp = relOp.compile(new TupleCompileContextImpl(tupleType), RelConfig.DEFAULT);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        if (RelOpCoder.INSTANCE.visit(relOp, outputStream) != CodingFlag.OK) {
            throw new RuntimeException("Expr coder visit error, expr: " + expr + ", after compile " + dingoExpr);
        }
        return CoprocessorV2.builder()
            .originalSchema(SchemaWrapper.builder().schema(attrSchemas).build())
            .selectionColumns(attrSchemas.stream().map(Schema::getIndex).collect(Collectors.toList()))
            .relExpr(outputStream.toByteArray())
            .build();
    }

    @Mapping(source = "langchainExpr", target = "vectorCoprocessor")
    VectorSearchParameter mapping(io.dingodb.proxy.model.dto.VectorSearchParameter parameter);

    default VectorSearchParameter.SearchNest mapping(Search search) {
        return Optional.<VectorSearchParameter.SearchNest>ofNullable(mapping(search.getFlat()))
            .ifAbsentSet(mapping(search.getHnswParam()))
            .ifAbsentSet(mapping(search.getIvfFlatParam()))
            .ifAbsentSet(mapping(search.getIvfPqParam()))
            .ifAbsentSet(mapping(search.getDiskAnnParam()))
            .orNull();
    }

    @Mappings({
        @Mapping(source = "floatValuesList", target = "floatValues"),
        @Mapping(source = "binaryValuesList", target = "binaryValues")
    })
    io.dingodb.sdk.service.entity.common.Vector mapping(ProxyCommon.Vector vector);

    void mapping(io.dingodb.sdk.service.entity.common.Vector vector, @MappingTarget ProxyCommon.Vector.Builder target);

    default ProxyCommon.Vector mapping(io.dingodb.sdk.service.entity.common.Vector vector) {
        ProxyCommon.Vector.Builder builder = ProxyCommon.Vector.newBuilder();
        mapping(vector, builder);
        if (vector.getFloatValues() != null && !vector.getFloatValues().isEmpty()) {
            builder.addAllFloatValues(vector.getFloatValues());
        }
        return builder.build();
    }

    IndexMetrics mapping(io.dingodb.sdk.service.entity.meta.IndexMetrics metrics);

    @Mapping(source = "scalarData", target = "scalarData.scalarData")
    io.dingodb.sdk.service.entity.common.VectorWithId mapping(ProxyCommon.VectorWithId vector);

    @Mapping(source = "fieldsList", target = "fields")
    ScalarValue mapping(ProxyCommon.ScalarValue scalarValue);

    default byte[] mapping(ByteString s) {
        return s.toByteArray();
    }

    void mapping(FlatParameter param, @MappingTarget CreateFlatParam.Builder target);

    void mapping(IvfFlatParameter param, @MappingTarget CreateIvfFlatParam.Builder target);

    void mapping(IvfPqParameter param, @MappingTarget CreateIvfPqParam.Builder target);

    void mapping(HnswParameter param, @MappingTarget CreateHnswParam.Builder target);

    void mapping(DiskannParameter param, @MappingTarget CreateDiskAnnParam.Builder target);

    void mapping(BruteforceParameter param, @MappingTarget CreateBruteForceParam.Builder target);

    FlatParameter mapping(CreateFlatParam param);

    IvfFlatParameter mapping(CreateIvfFlatParam param);

    IvfPqParameter mapping(CreateIvfPqParam param);

    HnswParameter mapping(CreateHnswParam param);

    DiskannParameter mapping(CreateDiskAnnParam param);

    BruteforceParameter mapping(CreateBruteForceParam param);

    Diskann mapping(SearchDiskAnnParam param);

    Flat mapping(SearchFlatParam param);

    IvfFlat mapping(SearchIvfFlatParam param);

    IvfPq mapping(SearchIvfPqParam param);

    Hnsw mapping(SearchHnswParam param);

    Diskann mapping(ProxyCommon.SearchDiskAnnParam param);

    Flat mapping(ProxyCommon.SearchFlatParam param);

    IvfFlat mapping(ProxyCommon.SearchIvfFlatParam param);

    IvfPq mapping(ProxyCommon.SearchIvfPqParam param);

    Hnsw mapping(ProxyCommon.SearchHNSWParam param);

//    default CoprocessorV2 mapping(String expr) {
//
//    }

    @Mappings({
        @Mapping(source = "parameter", target = "search"),
        @Mapping(source = "langchainExpr", target = "vectorCoprocessor")
    })
    VectorSearchParameter mapping(ProxyCommon.VectorSearchParameter parameter);

    default VectorSearchParameter.SearchNest mappingSearchNest(ProxyCommon.VectorSearchParameter parameter) {
        if (parameter.hasIvfPq()) {
            return mapping(parameter.getIvfPq());
        }
        if (parameter.hasIvfFlat()) {
            return mapping(parameter.getIvfFlat());
        }
        if (parameter.hasHnsw()) {
            return mapping(parameter.getHnsw());
        }
        if (parameter.hasFlat()) {
            return mapping(parameter.getFlat());
        }
        if (parameter.hasDiskann()) {
            return mapping(parameter.getDiskann());
        }
        return null;
    }

    default Vector mapping(Vector vector) {
        if (vector.getValueType() == Vector.ValueType.BINARY) {
            return Vector.getBinaryInstance(vector.getDimension(), vector.getBinaryValues());
        } else {
            return Vector.getFloatInstance(vector.getDimension(), vector.getFloatValues());
        }

    }

    default Map<String, byte[]> mapping(Map<String, String> metaData) {
        return metaData.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getBytes(StandardCharsets.UTF_8)));
    }

    default Map<String, String> mapping(Properties properties) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            result.put(key, value);
        }
        return result;
    }

    io.dingodb.proxy.model.dto.VectorDistanceArray mapping(VectorDistanceArray vectorDistanceArray);
}
