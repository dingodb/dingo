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

import io.dingodb.common.CommonId;
import io.dingodb.common.CommonId.CommonType;
import io.dingodb.common.store.KeyValue;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.service.entity.common.CoprocessorV2;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.RangeWithOptions;
import io.dingodb.sdk.service.entity.common.Schema;
import io.dingodb.sdk.service.entity.common.Type;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.EntityType;
import io.dingodb.sdk.service.entity.store.AggregationOperator;
import io.dingodb.sdk.service.entity.store.AggregationType;
import io.dingodb.sdk.service.entity.store.Coprocessor;
import io.dingodb.store.api.StoreInstance;
import io.dingodb.store.proxy.service.CodecService;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.dingodb.store.proxy.mapper.Mapper.MAPPER;

public interface EntityMapper {

    default CommonId idFrom(DingoCommonId id) {
        return new CommonId(CommonType.of(id.getEntityType().number), id.getParentEntityId(), id.getEntityId());
    }

    default DingoCommonId idTo(CommonId id) {
        return DingoCommonId.builder()
            .entityType(EntityType.forNumber(id.type.code))
            .parentEntityId(id.domain)
            .entityId(id.seq)
            .build();
    }

    CommonId copyId(CommonId id);

    DingoCommonId copyId(DingoCommonId id);

    List<CommonId> idFrom(Collection<DingoCommonId> ids);

    List<DingoCommonId> idTo(Collection<CommonId> ids);

    default Map<String, String> mapping(Properties properties) {
        Map<String, String> result = new HashMap<>();
        if (properties == null) {
            return result;
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            result.put(key, value);
        }
        return result;
    }

    io.dingodb.sdk.service.entity.common.KeyValue kvTo(KeyValue keyValue);

    KeyValue kvFrom(io.dingodb.sdk.service.entity.common.KeyValue keyValue);

    Range copyRange(StoreInstance.Range range);

    default RangeWithOptions rangeTo(long id, StoreInstance.Range range) {
        return RangeWithOptions.builder()
            .withStart(range.withStart)
            .withEnd(range.withEnd)
            .range(io.dingodb.sdk.service.entity.common.Range.builder()
                .startKey(CodecService.INSTANCE.setId(range.start, id))
                .endKey(CodecService.INSTANCE.setId(range.end, id))
                .build()
            ).build();
    }

    @Mappings({
        @Mapping(target = "selectionColumns", source = "selection"),
        @Mapping(target = "groupByColumns", source = "groupBy"),
        @Mapping(target = "aggregationOperators", source = "aggregations")
    })
    Coprocessor coprocessorTo(io.dingodb.common.Coprocessor coprocessor);

    @Mappings({
        @Mapping(target = "selectionColumns", source = "selection"),
    })
    CoprocessorV2 coprocessorTo(io.dingodb.common.CoprocessorV2 coprocessor);

    default io.dingodb.sdk.service.entity.store.SchemaWrapper toSchemaWrapper(io.dingodb.common.SchemaWrapper schemaWrapper) {
        return io.dingodb.sdk.service.entity.store.SchemaWrapper.builder()
            .schema(toSchemas(CodecUtils.createSchemaForColumnDefinitions(MAPPER.columnsTo(schemaWrapper.getSchemas()))))
            .commonId(schemaWrapper.getCommonId())
            .build();
    }

    default io.dingodb.sdk.service.entity.common.SchemaWrapper toSchemaWrapper1(io.dingodb.common.SchemaWrapper schemaWrapper) {
        return io.dingodb.sdk.service.entity.common.SchemaWrapper.builder()
            .schema(toSchemas(CodecUtils.createSchemaForColumnDefinitions(MAPPER.columnsTo(schemaWrapper.getSchemas()))))
            .commonId(schemaWrapper.getCommonId())
            .build();
    }

    default AggregationOperator aggregationOperatorTo(io.dingodb.common.AggregationOperator aggregationOperator) {
        return AggregationOperator.builder()
            .oper(AggregationType.forNumber(aggregationOperator.operation.getCode()))
            .indexOfColumn(aggregationOperator.indexOfColumn)
            .build();
    }

    default List<Schema> toSchemas(List<DingoSchema> schemas) {
        return schemas.stream()
            .map(schema -> {
                Type type;
                switch (schema.getType()) {
                    case BOOLEAN:
                        type = Type.BOOL;
                        break;
                    case INTEGER:
                        type = Type.INTEGER;
                        break;
                    case FLOAT:
                        type = Type.FLOAT;
                        break;
                    case LONG:
                        type = Type.LONG;
                        break;
                    case DOUBLE:
                        type = Type.DOUBLE;
                        break;
                    case BYTES:
                    case STRING:
                        type = Type.STRING;
                        break;
                    case BOOLEANLIST:
                        type = Type.BOOLLIST;
                        break;
                    case INTEGERLIST:
                        type = Type.INTEGERLIST;
                        break;
                    case FLOATLIST:
                        type = Type.FLOATLIST;
                        break;
                    case LONGLIST:
                        type = Type.LONGLIST;
                        break;
                    case DOUBLELIST:
                        type = Type.DOUBLELIST;
                        break;
                    case STRINGLIST:
                        type = Type.STRINGLIST;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + schema.getType());
                }
                return Schema.builder()
                    .type(type)
                    .isKey(schema.isKey())
                    .isNullable(schema.isAllowNull())
                    .index(schema.getIndex())
                    .build();
            }).collect(Collectors.toList());
    }

}
