package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;

import java.time.Instant;

public class InstantMapper extends TypeMapper {

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        Instant instant = (Instant) value;
        return instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        long longValue = (Long) value;
        return Instant.ofEpochSecond(longValue / 1_000_000_000, longValue % 1_000_000_000);
    }
}
