package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;

public class ByteMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).longValue();
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).byteValue();
    }
}
