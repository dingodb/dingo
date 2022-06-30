package io.dingodb.sdk.mappers;


import io.dingodb.sdk.utils.TypeMapper;
import io.dingodb.sdk.utils.TypeUtils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayMapper extends TypeMapper {

    private final Class<?> instanceClass;
    private final boolean supportedWithoutTranslation;
    private final TypeMapper instanceClassMapper;
    private final Boolean allowBatch;

    public ArrayMapper(final Class<?> instanceClass, final TypeMapper instanceClassMapper, final boolean allowBatch) {
        this.instanceClass = instanceClass;
        this.supportedWithoutTranslation = TypeUtils.isByteType(instanceClass);
        this.instanceClassMapper = instanceClassMapper;
        this.allowBatch = allowBatch;
    }

    @Override
    public Object toDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        int length = Array.getLength(value);
        if (this.supportedWithoutTranslation) {
            return value;
        }

        List<Object> results = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            results.add(this.instanceClassMapper.toDingoFormat(Array.get(value, i)));
        }
        return results;
    }

    @Override
    public Object fromDingoFormat(Object value) {
        if (value == null) {
            return null;
        }
        List<?> list = (List<?>) value;
        if (this.supportedWithoutTranslation) {
            return value;
        }

        Object result = Array.newInstance(instanceClass, list.size());
        for (int i = 0; i < list.size(); i++) {

            Object obj = list.get(i);
            Object item = this.instanceClassMapper.fromDingoFormat(obj);
            Array.set(result, i, item);
            /*
            if (!allowBatch || (!(item instanceof DeferredObject))) {
                Array.set(result, i, item);
            } else {
                final int thisIndex = i;
                DeferredSetter setter = object -> Array.set(result, thisIndex, object);
                DeferredObjectLoader.add(new DeferredObjectSetter(setter, (DeferredObject) item));
            }
            */
        }
        return result;
    }
}
