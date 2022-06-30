package io.dingodb.sdk.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

@Getter
@Setter
public class ClassConfig {
    @JsonProperty(value = "class")
    private String className;

    private String database;
    private String table;

    private KeyConfig key;
    private String factoryClass;
    private String factoryMethod;
    private final List<ColumnConfig> columns;

    public ClassConfig() {
        columns = new ArrayList<>();
    }

    public ColumnConfig getColumnByName(@NotNull String name) {
        for (ColumnConfig thisBin : columns) {
            if (name.equals(thisBin.getName())) {
                return thisBin;
            }
        }
        return null;
    }

    public ColumnConfig getColumnByGetterName(@NotNull String getterName) {
        for (ColumnConfig thisBin : columns) {
            if (getterName.equals(thisBin.getGetter())) {
                return thisBin;
            }
        }
        return null;
    }

    public ColumnConfig getColumnByFieldName(@NotNull String fieldName) {
        for (ColumnConfig thisBin : columns) {
            if (fieldName.equals(thisBin.getField())) {
                return thisBin;
            }
        }
        return null;
    }

    public void validate() {
        for (ColumnConfig thisBin : columns) {
            thisBin.validate(this.className);
        }
    }
}
