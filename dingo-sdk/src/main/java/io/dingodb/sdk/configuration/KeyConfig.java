package io.dingodb.sdk.configuration;

import org.apache.commons.lang3.StringUtils;

public class KeyConfig {
    private String field;
    private String getter;
    private String setter;

    public String getField() {
        return field;
    }

    public String getGetter() {
        return getter;
    }

    public String getSetter() {
        return setter;
    }

    public boolean isGetter(String methodName) {
        return (!StringUtils.isBlank(this.getter)) && this.getter.equals(methodName);
    }

    public boolean isSetter(String methodName) {
        return (!StringUtils.isBlank(this.setter)) && this.setter.equals(methodName);
    }
}
