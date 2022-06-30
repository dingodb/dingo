package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;

import java.util.Date;

public class DateMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        return ((Date) value).getTime();
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        long longValue = (Long) value;
        return new Date(longValue);
    }
}
