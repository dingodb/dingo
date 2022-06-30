package io.dingodb.sdk.mappers;

import io.dingodb.sdk.utils.TypeMapper;

public class LongMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        return value;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        return value;
    }
}
