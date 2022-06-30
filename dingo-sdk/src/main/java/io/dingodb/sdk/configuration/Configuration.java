package io.dingodb.sdk.configuration;

import java.util.ArrayList;
import java.util.List;

public class Configuration {
    private final List<ClassConfig> classes;

    public Configuration() {
        this.classes = new ArrayList<>();
    }

    public List<ClassConfig> getClasses() {
        return classes;
    }
}
