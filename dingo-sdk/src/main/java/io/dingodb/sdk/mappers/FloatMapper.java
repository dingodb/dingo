package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;

public class FloatMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        return value;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Number) value).floatValue();
    }
}
