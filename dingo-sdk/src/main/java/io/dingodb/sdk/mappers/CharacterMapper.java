package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;

public class CharacterMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        } else {
            char c = (Character) value;
            return (long) c;
        }
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        long longVal = ((Number) value).longValue();
        return (char) longVal;
    }
}
