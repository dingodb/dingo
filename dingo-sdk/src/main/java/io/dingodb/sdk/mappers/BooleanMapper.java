package io.dingodb.sdk.mappers;

import io.dingodb.sdk.utils.TypeMapper;

public class BooleanMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Boolean) value) ? 1 : 0;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return !Long.valueOf(0).equals(value);
    }
}
