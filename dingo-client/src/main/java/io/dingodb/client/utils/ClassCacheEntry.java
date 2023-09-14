/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.client.utils;

import io.dingodb.client.IBaseDingoMapper;
import io.dingodb.client.annotation.DingoColumn;
import io.dingodb.client.annotation.DingoConstructor;
import io.dingodb.client.annotation.DingoExclude;
import io.dingodb.client.annotation.DingoGetter;
import io.dingodb.client.annotation.DingoKey;
import io.dingodb.client.annotation.DingoOrdinal;
import io.dingodb.client.annotation.DingoRecord;
import io.dingodb.client.annotation.DingoSetter;
import io.dingodb.client.annotation.ParamFrom;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.PropertyDefinition;
import io.dingodb.client.common.Record;
import io.dingodb.client.common.SqlTypeInfo;
import io.dingodb.client.common.Value;
import io.dingodb.client.common.ValueType;
import io.dingodb.client.configuration.ClassConfig;
import io.dingodb.client.configuration.ColumnConfig;
import io.dingodb.client.configuration.KeyConfig;
import io.dingodb.client.mappers.TypeMapper;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.utils.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static io.dingodb.client.utils.TypeUtils.getSqlType;
import static io.dingodb.common.Common.Engine.ENG_ROCKSDB;

@Slf4j
public class ClassCacheEntry<T> {

    public static final String VERSION_PREFIX = "@V";
    public static final String TYPE_PREFIX = "@T:";
    public static final String TYPE_NAME = ".type";
    private String database;
    private String tableName;

    private int version = 1;
    private final Class<T> clazz;
    private List<ValueType> keys = new ArrayList<>();
    private boolean mapAll = true;
    private List<String> keysName = new ArrayList<>();
    private final LinkedHashMap<String, ValueType> values = new LinkedHashMap<>();
    private ClassCacheEntry<?> superClazz;
    private int columnCnt;
    private final IBaseDingoMapper mapper;

    private Map<Integer, String> ordinals = null;

    private Set<String> fieldsWithOrdinals = null;

    private final ClassConfig classConfig;
    private String[] constructorParamBins;
    private Object[] constructorParamDefaults;
    private Constructor<T> constructor;
    private final ClassConfig config;
    private String factoryMethod;
    private String factoryClass;

    private enum FactoryMethodType {
        NO_PARAMS,
        CLASS,
        MAP,
        CLASS_MAP
    }

    private Method factoryConstructorMethod;
    private FactoryMethodType factoryConstructorType;

    /**
     * When there are subclasses, need to store type information to be able to re-create an instance of the same type.
     * As the class name can be verbose, we provide the ability to set a string representing the class name.
     * This string must be unique for all classes.
     */
    private String shortenedClassName;
    private boolean isChildClass = false;

    private volatile boolean constructed;

    // package visibility only.
    ClassCacheEntry(@NotNull Class<T> clazz, IBaseDingoMapper mapper, ClassConfig config) {
        this.clazz = clazz;
        this.mapper = mapper;
        this.classConfig = config;

        DingoRecord recordDescription = clazz.getAnnotation(DingoRecord.class);
        if (recordDescription == null && config == null) {
            throw new NotAnnotatedClass("Class " + clazz.getName()
                + " is not augmented by the @DingoRecord annotation");
        } else if (recordDescription != null) {
            this.database = ParserUtils.getInstance().get(recordDescription.database());
            this.tableName = ParserUtils.getInstance().get(recordDescription.table().toUpperCase());
            this.factoryClass = recordDescription.factoryClass();
            this.factoryMethod = recordDescription.factoryMethod();
        }
        this.config = config;
    }

    public ClassCacheEntry<T> construct() {
        if (config != null) {
            config.validate();
            this.overrideSettings(config);
        }

        this.loadFieldsFromClass();
        this.loadPropertiesFromClass();
        this.superClazz = ClassCache.getInstance().loadClass(this.clazz.getSuperclass(), this.mapper);
        this.columnCnt = this.values.size() + (superClazz != null ? superClazz.columnCnt : 0);
        if (this.columnCnt == 0) {
            throw new DingoClientException("Class "
                + clazz.getSimpleName()
                + " has no values defined to be stored in the database");
        }
        this.formOrdinalsFromValues();
        Method factoryConstructorMethod = findConstructorFactoryMethod();
        if (factoryConstructorMethod == null) {
            this.findConstructor();
        } else {
            this.setConstructorFactoryMethod(factoryConstructorMethod);
        }
        if (StringUtils.isBlank(this.shortenedClassName)) {
            this.shortenedClassName = clazz.getSimpleName();
        }
        ClassCache.getInstance().setStoredName(this, this.shortenedClassName);

        this.checkRecordSettingsAgainstSuperClasses();
        constructed = true;
        return this;
    }

    public boolean isNotConstructed() {
        return !constructed;
    }

    public Class<?> getUnderlyingClass() {
        return this.clazz;
    }

    public ClassConfig getClassConfig() {
        return this.classConfig;
    }

    public String getShortenedClassName() {
        return this.shortenedClassName;
    }

    private void overrideSettings(ClassConfig config) {
        if (!StringUtils.isBlank(config.getDatabase())) {
            this.database = config.getDatabase();
        }
        if (!StringUtils.isBlank(config.getTable())) {
            this.tableName = config.getTable();
        }
        if (config.getFactoryMethod() != null) {
            this.factoryMethod = config.getFactoryMethod();
        }
        if (config.getFactoryClass() != null) {
            this.factoryClass = config.getFactoryClass();
        }
    }

    public boolean isChildClass() {
        return isChildClass;
    }

    private void checkRecordSettingsAgainstSuperClasses() {
        if (!StringUtils.isBlank(this.database) && !StringUtils.isBlank(this.tableName)) {
            // This class defines it's own database + table,
            // it is only a child class if it's closest named superclass is the same as ours.
            this.isChildClass = false;
            ClassCacheEntry<?> thisEntry = this.superClazz;
            while (thisEntry != null) {
                boolean isOK = (!StringUtils.isBlank(thisEntry.getDatabase()))
                    && (!StringUtils.isBlank(thisEntry.getTableName()));
                if (isOK) {
                    if (this.database.equals(thisEntry.getDatabase())
                        && this.tableName.equals(thisEntry.getTableName())) {
                        this.isChildClass = true;
                    }
                    break;
                }
                thisEntry = thisEntry.superClazz;
            }
        } else {
            // Otherwise this is a child class, find the set and namespace from the closest highest class
            this.isChildClass = true;
            ClassCacheEntry<?> thisEntry = this.superClazz;
            while (thisEntry != null) {
                if ((!StringUtils.isBlank(thisEntry.getDatabase()))
                    && (!StringUtils.isBlank(thisEntry.getTableName()))) {
                    this.database = thisEntry.getDatabase();
                    this.tableName = thisEntry.getTableName();
                    break;
                }
                thisEntry = thisEntry.superClazz;
            }
        }
        ClassCacheEntry<?> thisEntry = this.superClazz;
        while (thisEntry != null) {
            if ( thisEntry.keys != null) {
                this.keys = thisEntry.keys;
            }
            thisEntry = thisEntry.superClazz;
        }
    }

    private ColumnConfig getColumnFromName(String name) {
        if (this.classConfig == null || this.classConfig.getColumns() == null) {
            return null;
        }
        for (ColumnConfig thisColumn : this.classConfig.getColumns()) {
            if (thisColumn.getDerivedName().equals(name)) {
                return thisColumn;
            }
        }
        return null;
    }

    private ColumnConfig getColumnFromField(Field field) {
        if (this.classConfig == null || this.classConfig.getColumns() == null) {
            return null;
        }
        for (ColumnConfig thisBin : this.classConfig.getColumns()) {
            if (thisBin.getField() != null && thisBin.getField().equals(field.getName())) {
                return thisBin;
            }
        }
        return null;
    }

    private ColumnConfig getColumnFromGetter(String name) {
        if (this.classConfig == null || this.classConfig.getColumns() == null) {
            return null;
        }
        for (ColumnConfig thisBin : this.classConfig.getColumns()) {
            if (thisBin.getGetter() != null && thisBin.getGetter().equals(name)) {
                return thisBin;
            }
        }
        return null;
    }

    private ColumnConfig getColumnFromSetter(String name) {
        if (this.classConfig == null || this.classConfig.getColumns() == null) {
            return null;
        }
        for (ColumnConfig thisBin : this.classConfig.getColumns()) {
            if (thisBin.getSetter() != null && thisBin.getSetter().equals(name)) {
                return thisBin;
            }
        }
        return null;
    }

    private void formOrdinalsFromValues() {
        for (String thisValueName : this.values.keySet()) {
            ValueType thisValue = this.values.get(thisValueName);

            ColumnConfig columnConfig = getColumnFromName(thisValueName);
            Integer ordinal = columnConfig == null ? null : columnConfig.getOrdinal();

            if (ordinal == null) {
                for (Annotation thisAnnotation : thisValue.getAnnotations()) {
                    if (thisAnnotation instanceof DingoOrdinal) {
                        ordinal = ((DingoOrdinal) thisAnnotation).value();
                    }
                    break;
                }
            }
            if (ordinal != null) {
                if (ordinals == null) {
                    ordinals = new HashMap<>();
                    fieldsWithOrdinals = new HashSet<>();
                }
                if (ordinals.containsKey(ordinal)) {
                    throw new DingoClientException(String.format("Class %s has multiple values with the ordinal of %d",
                        clazz.getSimpleName(), ordinal));
                }
                ordinals.put(ordinal, thisValueName);
                fieldsWithOrdinals.add(thisValueName);
            }
        }

        if (ordinals != null) {
            // The ordinals need to be valued from 1..<numOrdinals>
            for (int i = 1; i <= ordinals.size(); i++) {
                if (!ordinals.containsKey(i)) {
                    throw new DingoClientException(String.format("Class %s has %d values specifying ordinals."
                            + " These should be 1..%d, but %d is missing",
                        clazz.getSimpleName(), ordinals.size(), ordinals.size(), i));
                }
            }
        }
    }

    private boolean validateFactoryMethod(Method method) {
        if ((method.getModifiers() & Modifier.STATIC) == Modifier.STATIC
            && this.factoryMethod.equals(method.getName())) {
            Parameter[] params = method.getParameters();
            if (params.length == 0) {
                return true;
            }
            if (params.length == 1
                && ((Class.class.isAssignableFrom(params[0].getType()))
                || Map.class.isAssignableFrom(params[0].getType()))
            ) {
                return true;
            }
            if (params.length == 2
                && Class.class.isAssignableFrom(params[0].getType())
                && Map.class.isAssignableFrom(params[1].getType())) {
                return true;
            }
        }
        return false;
    }

    private Method findConstructorFactoryMethod() {
        if (!StringUtils.isBlank(this.factoryClass) || !StringUtils.isBlank(this.factoryMethod)) {
            // Both must be specified
            if (StringUtils.isBlank(this.factoryClass)) {
                String errorMsg = "Missing factoryClass definition when factoryMethod is specified on class "
                    + clazz.getSimpleName();
                throw new DingoClientException(errorMsg);
            }
            if (StringUtils.isBlank(this.factoryClass)) {
                String errorMsg = "Missing factoryMethod definition when factoryClass is specified on class "
                    + clazz.getSimpleName();
                throw new DingoClientException(errorMsg);
            }
            // Load the class and check for the method
            try {
                Class<?> factoryClazzType = Class.forName(this.factoryClass);
                Method foundMethod = null;
                for (Method method : factoryClazzType.getDeclaredMethods()) {
                    if (validateFactoryMethod(method)) {
                        if (foundMethod != null) {
                            throw new DingoClientException(String.format("Factory Class %s defines at least 2 valid "
                                    + "factory methods (%s, %s) as a factory for class %s",
                                this.factoryClass, foundMethod, method, this.clazz.getSimpleName()));
                        }
                        foundMethod = method;
                    }
                }
                if (foundMethod == null) {
                    throw new DingoClientException(String.format("Class %s specified a factory class of %s and "
                            + "a factory method of %s, but no valid method with that name exists on the class. A valid"
                            + " method must be static, can take no parameters, a single Class parameter, a single"
                            + " Map parameter, or a Class and a Map parameter, and must return an object which is"
                            + " either an ancestor, descendant or equal to %s",
                        clazz.getSimpleName(), this.factoryClass, this.factoryMethod, clazz.getSimpleName()));
                }
                return foundMethod;
            } catch (ClassNotFoundException cnfe) {
                throw new DingoClientException(String.format("Factory class %s for class %s cannot be loaded",
                    this.factoryClass, clazz.getSimpleName()));
            }
        }
        return null;
    }

    /**
     * Set up the details of the constructor factory method. The method must be returned from the
     * <code>findConstructorFactoryMethod</code> above to ensure it is valid.
     *
     * @param method The factory method to set.
     */
    private void setConstructorFactoryMethod(Method method) {
        this.factoryConstructorMethod = method;
        this.factoryConstructorMethod.setAccessible(true);

        if (method.getParameterCount() == 0) {
            this.factoryConstructorType = FactoryMethodType.NO_PARAMS;
        } else if (method.getParameterCount() == 2) {
            this.factoryConstructorType = FactoryMethodType.CLASS_MAP;
        } else if (Class.class.isAssignableFrom(method.getParameters()[0].getType())) {
            this.factoryConstructorType = FactoryMethodType.CLASS;
        } else {
            this.factoryConstructorType = FactoryMethodType.MAP;
        }
    }

    @SuppressWarnings("unchecked")
    private void findConstructor() {
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        if (constructors.length == 0) {
            throw new DingoClientException("Class " + clazz.getSimpleName()
                + " has no constructors and hence cannot be mapped to Dingo");
        }
        Constructor<?> desiredConstructor = null;
        Constructor<?> noArgConstructor = null;
        if (constructors.length == 1) {
            desiredConstructor = constructors[0];
        } else {
            for (Constructor<?> thisConstructor : constructors) {
                if (thisConstructor.getParameters().length == 0) {
                    noArgConstructor = thisConstructor;
                }
                DingoConstructor dingoConstructor = thisConstructor.getAnnotation(DingoConstructor.class);
                if (dingoConstructor != null) {
                    if (desiredConstructor != null) {
                        throw new DingoClientException("Class " + clazz.getSimpleName()
                            + " has multiple constructors annotated with @DingoConstructor. "
                            + "Only one constructor can be so annotated.");
                    } else {
                        desiredConstructor = thisConstructor;
                    }
                }
            }
        }
        if (desiredConstructor == null && noArgConstructor != null) {
            constructorParamBins = new String[0];
            desiredConstructor = noArgConstructor;
        }

        if (desiredConstructor == null) {
            throw new DingoClientException("Class " + clazz.getSimpleName()
                + " has neither a no-arg constructor, "
                + "nor a constructor annotated with @DingoConstructor so cannot be mapped to Dingo.");
        }

        Parameter[] params = desiredConstructor.getParameters();
        this.constructorParamBins = new String[params.length];
        this.constructorParamDefaults = new Object[params.length];

        Map<String, ValueType> allValues = new HashMap<>();
        ClassCacheEntry<?> current = this;
        while (current != null) {
            allValues.putAll(current.values);
            current = current.superClazz;
        }
        int count = 0;

        // Parameters can be either specified by their name (which requires the use of the javac -parameters flag),
        // or through an @ParamFrom annotation.
        for (Parameter thisParam : params) {
            count++;
            boolean isFromAnnotation = false;
            String columnName = thisParam.getName();
            ParamFrom parameterDetails = thisParam.getAnnotation(ParamFrom.class);

            if (parameterDetails != null) {
                columnName = parameterDetails.value();
                isFromAnnotation = true;
            }

            // Validate that we have such a value
            if (!allValues.containsKey(columnName)) {
                String valueList = String.join(",", values.keySet());
                boolean isDefaultAnnotation = (!isFromAnnotation && columnName.startsWith("arg"));
                String message = String.format("Class %s has a preferred constructor of %s. However, parameter %d is "
                        + "mapped to column \"%s\" %s which is not one of the values on the class, which are: %s%s",
                    clazz.getSimpleName(), desiredConstructor, count, columnName,
                    isFromAnnotation ? "via the @ParamFrom annotation" : "via the argument name",
                    valueList,
                    (isDefaultAnnotation) ? ". forget to specify '-parameters' to javac when building?" : "");
                throw new DingoClientException(message);
            }
            Class<?> type = thisParam.getType();
            if (!type.isAssignableFrom(allValues.get(columnName).getType())) {
                throw new DingoClientException("Class " + clazz.getSimpleName() + " has a preferred constructor of "
                    + desiredConstructor + ". However, parameter " + count
                    + " is of type " + type + " but assigned from column \"" + columnName + "\" of type "
                    + values.get(columnName).getType() + ". These types are incompatible.");
            }
            constructorParamBins[count - 1] = columnName;
            constructorParamDefaults[count - 1] = PrimitiveDefaults.getDefaultValue(thisParam.getType());
        }
        this.constructor = (Constructor<T>) desiredConstructor;
        this.constructor.setAccessible(true);
    }

    private PropertyDefinition getOrCreateProperty(String name, Map<String, PropertyDefinition> properties) {
        PropertyDefinition thisProperty = properties.get(name);
        if (thisProperty == null) {
            thisProperty = new PropertyDefinition(name, mapper);
            properties.put(name, thisProperty);
        }
        return thisProperty;
    }

    private void loadPropertiesFromClass() {
        Map<String, PropertyDefinition> properties = new HashMap<>();
        PropertyDefinition keyProperty = null;
        KeyConfig keyConfig = config != null ? config.getKey() : null;
        for (Method thisMethod : clazz.getDeclaredMethods()) {

            String methodName = thisMethod.getName();
            ColumnConfig getterConfig = getColumnFromGetter(methodName);
            ColumnConfig setterConfig = getColumnFromSetter(methodName);

            boolean isKeyViaConfig = keyConfig != null
                && (keyConfig.isGetter(methodName) || keyConfig.isSetter(methodName));
            if (thisMethod.isAnnotationPresent(DingoKey.class) || isKeyViaConfig) {

                if (keyProperty == null) {
                    keyProperty = new PropertyDefinition("_key_", mapper);
                }
                if (isKeyViaConfig) {
                    if (keyConfig.isGetter(methodName)) {
                        keyProperty.setGetter(thisMethod);
                    } else {
                        keyProperty.setSetter(thisMethod);
                    }
                } else {
                    DingoKey key = thisMethod.getAnnotation(DingoKey.class);
                    if (key.setter()) {
                        keyProperty.setSetter(thisMethod);
                    } else {
                        keyProperty.setGetter(thisMethod);
                    }
                }
            }

            if (thisMethod.isAnnotationPresent(DingoGetter.class) || getterConfig != null) {
                String getterName = (getterConfig != null)
                    ? getterConfig.getName() : thisMethod.getAnnotation(DingoGetter.class).name();

                String name = ParserUtils.getInstance().get(ParserUtils.getInstance().get(getterName));
                PropertyDefinition thisProperty = getOrCreateProperty(name, properties);
                thisProperty.setGetter(thisMethod);
            }

            if (thisMethod.isAnnotationPresent(DingoSetter.class) || setterConfig != null) {
                String setterName = (setterConfig != null)
                    ? setterConfig.getName() : thisMethod.getAnnotation(DingoSetter.class).name();
                String name = ParserUtils.getInstance().get(ParserUtils.getInstance().get(setterName));
                PropertyDefinition thisProperty = getOrCreateProperty(name, properties);
                thisProperty.setSetter(thisMethod);
            }
        }

        for (String thisPropertyName : properties.keySet()) {
            PropertyDefinition thisProperty = properties.get(thisPropertyName);
            thisProperty.validate(clazz.getName(), config, false);
            if (this.values.get(thisPropertyName) != null) {
                throw new DingoClientException("Class " + clazz.getName() + " cannot define the mapped name "
                    + thisPropertyName + " more than once");
            }
            TypeUtils.AnnotatedType annotatedType = new TypeUtils.AnnotatedType(config, thisProperty.getGetter());
            TypeMapper typeMapper = TypeUtils.getMapper(thisProperty.getType(), annotatedType, this.mapper);
            ValueType value = new ValueType.MethodValue(thisProperty, typeMapper, annotatedType);
            values.put(thisPropertyName, value);
        }
    }

    private void loadFieldsFromClass() {
        KeyConfig keyConfig = config != null ? config.getKey() : null;
        String keyField = keyConfig == null ? null : keyConfig.getField();
        for (Field thisField : this.clazz.getDeclaredFields()) {
            boolean isKey = false;
            ColumnConfig thisBin = getColumnFromField(thisField);
            if (thisField.isAnnotationPresent(DingoKey.class)
                || (!StringUtils.isBlank(keyField) && keyField.equals(thisField.getName()))) {
                if (thisField.isAnnotationPresent(DingoExclude.class)
                    || (thisBin != null && thisBin.isExclude() != null && thisBin.isExclude())) {
                    throw new DingoClientException("Class " + clazz.getName()
                        + " cannot have a field which is both a key and excluded.");
                }
                TypeUtils.AnnotatedType annotatedType = new TypeUtils.AnnotatedType(config, thisField);
                TypeMapper typeMapper = TypeUtils.getMapper(thisField.getType(), annotatedType, this.mapper);
                keys.add(new ValueType.FieldValue(thisField, typeMapper, annotatedType));
                isKey = true;
            }

            if (thisField.isAnnotationPresent(DingoExclude.class)
                || (thisBin != null && thisBin.isExclude() != null && thisBin.isExclude())) {
                // This field should be excluded from being stored in the database. Even keys must be stored
                continue;
            }

            if (this.mapAll || thisField.isAnnotationPresent(DingoColumn.class) || thisBin != null) {
                // This field needs to be mapped
                DingoColumn dingoColumn = thisField.getAnnotation(DingoColumn.class);
                String columnName = dingoColumn == null ? null : ParserUtils.getInstance().get(dingoColumn.name());
                if (thisBin != null && !StringUtils.isBlank(thisBin.getDerivedName())) {
                    columnName = thisBin.getDerivedName();
                }
                String name;
                if (StringUtils.isBlank(columnName)) {
                    name = thisField.getName();
                } else {
                    name = columnName;
                }
                if (isKey) {
                    keysName.add(name);
                }

                if (this.values.get(name) != null) {
                    throw new DingoClientException("Class " + clazz.getName()
                        + " cannot define the mapped name " + name + " more than once");
                }
                if ((dingoColumn != null && dingoColumn.useAccessors())
                    || (thisBin != null && thisBin.getUseAccessors() != null && thisBin.getUseAccessors())) {
                    validateAccessorsForField(name, thisField);
                } else {
                    thisField.setAccessible(true);
                    TypeUtils.AnnotatedType annotatedType = new TypeUtils.AnnotatedType(config, thisField);
                    TypeMapper typeMapper = TypeUtils.getMapper(thisField.getType(), annotatedType, this.mapper);
                    ValueType valueType = new ValueType.FieldValue(thisField, typeMapper, annotatedType);
                    values.put(name, valueType);
                }
            }
        }
    }

    private Method findMethodWithNameAndParams(String name, Class<?>... params) {
        try {
            Method method = this.clazz.getDeclaredMethod(name, params);
            // TODO: Should this ascend the inheritance hierarchy using getMethod on superclasses?
            return method;
        } catch (NoSuchMethodException nsme) {
            return null;
        }
    }

    private void validateAccessorsForField(String columnName, Field thisField) {
        String fieldName = thisField.getName();
        String methodNameBase = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        String getterName = "get" + methodNameBase;
        String setterName = "set" + methodNameBase;

        Method getter = findMethodWithNameAndParams(getterName);
        if (getter == null) {
            throw new DingoClientException(String.format(
                "Expected to find getter for field %s on class %s due to it being configured to useAccessors, "
                    + "but no method with the signature \"%s %s()\" was found",
                fieldName,
                this.clazz.getSimpleName(),
                thisField.getType().getSimpleName(),
                getterName));
        }

        Method setter = findMethodWithNameAndParams(setterName, thisField.getType());
        if (setter == null) {
            setter = findMethodWithNameAndParams(setterName, thisField.getType(), Key.class);
        }
        if (setter == null) {
            setter = findMethodWithNameAndParams(setterName, thisField.getType(), Value.class);
        }
        if (setter == null) {
            throw new DingoClientException(String.format(
                "Expected to find setter for field %s on class %s due to it being configured to useAccessors,"
                    + " but no method with the name \"%s\" was found",
                fieldName,
                this.clazz.getSimpleName(),
                setterName));
        }

        TypeUtils.AnnotatedType annotatedType = new TypeUtils.AnnotatedType(config, thisField);
        TypeMapper typeMapper = TypeUtils.getMapper(thisField.getType(), annotatedType, this.mapper);
        PropertyDefinition property = new PropertyDefinition(columnName, mapper);
        property.setGetter(getter);
        property.setSetter(setter);
        property.validate(clazz.getName(), config, false);

        ValueType value = new ValueType.MethodValue(property, typeMapper, annotatedType);
        values.put(columnName, value);
    }

    public Object translateKeyToDingoKey(TypeMapper typeMapper, Object key) {
        return typeMapper.toDingoFormat(key);
    }

    private Object internalGetKey(Object object) throws ReflectiveOperationException {
        List<Object> keyList = new ArrayList<>();
        for (ValueType key : this.keys) {
            if (key != null) {
                keyList.add(this.translateKeyToDingoKey(key.getTypeMapper(), key.get(object)));
            }
        }
        return keyList;
    }

    public Object getKey(Object object) {
        try {
            Object key = this.internalGetKey(object);
            if (key instanceof List && ((List) key).size() == 0) {
                throw new DingoClientException("Null key from annotated object of class "
                    + this.clazz.getSimpleName() + ". Did you forget an @DingoKey annotation?");
            }
            return key;
        } catch (ReflectiveOperationException re) {
            throw new DingoClientException(re.getMessage());
        }
    }

    private void internalSetKey(Object[] objects, Object[] values) throws ReflectiveOperationException {
        // TODO:
        if (this.keys != null && this.keys.size() > 0) {
            for (int i = 0; i < keys.size(); i++) {
                keys.get(i).set(objects[i], values[i]);
            }
        } else if (superClazz != null) {
            this.superClazz.internalSetKey(objects, values);
        }
    }

    public void setKey(Object[] object, Object[] value) {
        try {
            this.internalSetKey(object, value);
        } catch (ReflectiveOperationException re) {
            throw new DingoClientException(re.getMessage());
        }
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName.toUpperCase();
    }

    private boolean contains(String[] names, String thisName) {
        if (names == null || names.length == 0) {
            return true;
        }
        if (thisName == null) {
            return false;
        }
        for (String aName : names) {
            if (thisName.equals(aName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * get all values from the record by instance of Object.
     *
     * @param instance         input object
     * @param allowNullColumns if true, null columns will be included in the result.
     * @return the whole values of the record.
     */
    public Value[] getValues(Object instance, boolean allowNullColumns) {
        Value[] values = new Value[this.columnCnt];
        try {
            int index = 0;
            ClassCacheEntry thisClass = this;
            while (thisClass != null) {
                Set<String> keys = thisClass.values.keySet();
                for (String name : keys) {
                    ValueType value = (ValueType) thisClass.values.get(name);
                    Object javaValue = value.get(instance);
                    Object dingoValue = value.getTypeMapper().toDingoFormat(javaValue);
                    if (dingoValue != null || allowNullColumns) {
                        if (dingoValue instanceof TreeMap<?, ?>) {
                            TreeMap<?, ?> treeMap = (TreeMap<?, ?>) dingoValue;
                            // columns[index++] = new Column(name, new ArrayList(treeMap.entrySet()),
                            // MapOrder.KEY_ORDERED);
                            values[index++] = Value.get(new ArrayList<>(treeMap.entrySet()));
                        } else {
                            values[index++] = Value.get(dingoValue);
                        }
                    }
                }
                thisClass = thisClass.superClazz;
            }
            return values;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Value[] getValues(Object newInstance,
                             Record oldRecord,
                             boolean allowNullColumns,
                             String[] includeColumns) {
        try {
            Value[] values = new Value[this.columnCnt];
            int index = 0;
            ClassCacheEntry thisClass = this;
            while (thisClass != null) {
                Set<String> keys = thisClass.values.keySet();
                for (String name : keys) {
                    Object dingoValue = null;
                    if (!contains(includeColumns, name)) {
                        dingoValue = oldRecord.getValue(name);
                    } else {
                        ValueType value = (ValueType) thisClass.values.get(name);
                        Object javaValue = value.get(newInstance);
                        dingoValue = value.getTypeMapper().toDingoFormat(javaValue);
                    }

                    if (dingoValue != null || allowNullColumns) {
                        if (dingoValue instanceof TreeMap<?, ?>) {
                            TreeMap<?, ?> treeMap = (TreeMap<?, ?>) dingoValue;
                            // columns[index++] = new Column(name, new ArrayList(treeMap.entrySet()),
                            // MapOrder.KEY_ORDERED);
                            values[index++] = Value.get(new ArrayList<>(treeMap.entrySet()));
                        } else {
                            values[index++] = Value.get(dingoValue);
                        }
                    }
                }
                thisClass = thisClass.superClazz;
            }
            return values;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    /**
     * check current input columns is all valid.
     *
     * @param includeColumns input columns to check.
     * @return true if all columns are valid, otherwise false.
     */
    public boolean isAllColumnsValid(String[] includeColumns) {
        Set<String> keys = this.values.keySet();
        for (String inColumn : includeColumns) {
            if (!keys.contains(inColumn)) {
                return true;
            }
        }
        return false;
    }


    private SqlTypeInfo getSqlTypeInfo(final String inputJavaType,
                                       Integer precision,
                                       Integer scale,
                                       String defaultValue) {
        String typeName = getSqlType(inputJavaType);
        return new SqlTypeInfo(typeName, precision, scale, defaultValue);
    }

    private SqlTypeInfo getSqlTypeInfo(final String inputJavaType) {
        String typeName = getSqlType(inputJavaType);
        return new SqlTypeInfo(typeName);
    }

    public TableDefinition getTableDefinition(String tableName) {
        List<Field> keyFieldList = this.keys.stream()
            .map(v -> ((ValueType.FieldValue) v).getField()).collect(Collectors.toList());
        int pkIndex = 0;
        List<Column> columnDefinitions = new ArrayList<>();
        for (Field thisField : this.clazz.getDeclaredFields()) {
            boolean isKey = false;
            if (thisField.isAnnotationPresent(DingoKey.class)) {
                if (thisField.isAnnotationPresent(DingoExclude.class)) {
                    throw new DingoClientException("Class " + clazz.getName()
                        + " cannot have a field which is both a key and excluded.");
                }

                if (this.keys != null && keyFieldList.contains(thisField)) {
                    isKey = true;
                }
            }

            if (thisField.isAnnotationPresent(DingoExclude.class)) {
                // This field should be excluded from being stored in the database. Even keys must be stored
                continue;
            }

            String javaTypeName = thisField.getType().getSimpleName();
            SqlTypeInfo sqlTypeInfo = getSqlTypeInfo(javaTypeName);

            String columnName = thisField.getName();
            String elementTypeName = null;
            if (thisField.isAnnotationPresent(DingoColumn.class)) {
                DingoColumn dingoColumn = thisField.getAnnotation(DingoColumn.class);
                String sqlTypeName = getSqlType(javaTypeName);
                sqlTypeInfo = new SqlTypeInfo(sqlTypeName, dingoColumn);
                if (dingoColumn.name() != null && !dingoColumn.name().isEmpty()) {
                    columnName = dingoColumn.name();
                }

                if (dingoColumn.elementType() != null && !dingoColumn.elementType().isEmpty()) {
                    elementTypeName = getSqlType(dingoColumn.elementType());
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Input JavaTypeName is:{}, sqlTypeInfo:{}, "
                        + "columnName:{}, elementTypeName:{}",
                    javaTypeName,
                    sqlTypeInfo,
                    thisField.getType().toGenericString(),
                    elementTypeName);
            }
            int primary = -1;
            if (isKey) {
                primary = pkIndex++;
            }
            ColumnDefinition columnDefinition = ColumnDefinition.builder()
                .name(columnName.toUpperCase())
                .type(sqlTypeInfo.getSqlTypeName())
                .elementType(elementTypeName)
                .precision(sqlTypeInfo.getPrecision() == null ? -1 : sqlTypeInfo.getPrecision())
                .scale(sqlTypeInfo.getScale() == null ? Integer.MIN_VALUE : sqlTypeInfo.getScale())
                .nullable(!isKey)
                .primary(primary)
                .defaultValue(sqlTypeInfo.getDefaultValue())
                .isAutoIncrement(false)
                .build();
            columnDefinitions.add(columnDefinition);
        }
        return TableDefinition.builder()
            .name(tableName)
            .columns(columnDefinitions)
            .engine(ENG_ROCKSDB.name())
            .build();
    }

    public Map<String, Object> getMap(Object instance, boolean needsType) {
        try {
            Map<String, Object> results = new HashMap<>();
            ClassCacheEntry<?> thisClass = this;
            if (needsType) {
                results.put(TYPE_NAME, this.getShortenedClassName());
            }
            while (thisClass != null) {
                for (String name : thisClass.values.keySet()) {
                    ValueType value = thisClass.values.get(name);
                    Object javaValue = value.get(instance);
                    Object dingoValue = value.getTypeMapper().toDingoFormat(javaValue);
                    results.put(name, dingoValue);
                }
                thisClass = thisClass.superClazz;
            }
            return results;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    private void addDataFromValueName(String name,
                                      Object instance,
                                      ClassCacheEntry<?> thisClass,
                                      List<Object> results) throws ReflectiveOperationException {
        ValueType value = thisClass.values.get(name);
        Object javaValue = value.get(instance);
        Object dingoValue = value.getTypeMapper().toDingoFormat(javaValue);
        results.add(dingoValue);
    }

    private boolean isKeyField(String name) {
        return keysName.contains(name);
    }

    public List<Object> getList(Object instance, boolean skipKey, boolean needsType) {
        try {
            List<Object> results = new ArrayList<>();
            List<Object> versionsToAdd = new ArrayList<>();
            ClassCacheEntry<?> thisClass = this;
            while (thisClass != null) {
                if (thisClass.version > 1) {
                    versionsToAdd.add(0, VERSION_PREFIX + thisClass.version);
                }
                if (thisClass.ordinals != null) {
                    for (int i = 1; i <= thisClass.ordinals.size(); i++) {
                        String name = thisClass.ordinals.get(i);
                        if (!skipKey || !isKeyField(name)) {
                            addDataFromValueName(name, instance, thisClass, results);
                        }
                    }
                }
                for (String name : thisClass.values.keySet()) {
                    if (thisClass.fieldsWithOrdinals == null || !thisClass.fieldsWithOrdinals.contains(name)) {
                        if (!skipKey || !isKeyField(name)) {
                            addDataFromValueName(name, instance, thisClass, results);
                        }
                    }
                }
                thisClass = thisClass.superClazz;
            }
            results.addAll(versionsToAdd);
            if (needsType) {
                results.add(TYPE_PREFIX + this.getShortenedClassName());
            }
            return results;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    public T constructAndHydrate(Map<String, Object> map) {
        return constructAndHydrate(null, map);
    }

    public T constructAndHydrate(Record record) {
        return constructAndHydrate(record, null);
    }

    @SuppressWarnings("unchecked")
    public T constructAndHydrate(List<Object> list, boolean skipKey) {
        Map<String, Object> valueMap = new HashMap<>();
        try {
            ClassCacheEntry<?> thisClass = this;
            int index = 0;
            int endIndex = list.size();
            if (!list.isEmpty()) {
                // If the object saved in the list was a subclass of the declared type,
                // it must have the type name as the last element of the list.
                // Note that there is a performance implication of using subclasses.
                Object obj = list.get(endIndex - 1);
                if ((obj instanceof String) && ((String) obj).startsWith(TYPE_PREFIX)) {
                    String className = ((String) obj).substring(TYPE_PREFIX.length());
                    thisClass = ClassCache.getInstance().getCacheEntryFromStoredName(className);
                    if (thisClass == null) {
                        Class<?> typeClazz = Class.forName(className);
                        thisClass = ClassCache.getInstance().loadClass(typeClazz, this.mapper);
                    }
                    endIndex--;
                }
            }

            T result = null;
            while (thisClass != null) {
                if (index < endIndex) {
                    Object lastValue = list.get(endIndex - 1);
                    int recordVersion = 1;
                    if ((lastValue instanceof String) && (((String) lastValue).startsWith(VERSION_PREFIX))) {
                        recordVersion = Integer.parseInt(((String) lastValue).substring(2));
                        endIndex--;
                    }
                    int objectVersion = thisClass.version;
                    if (thisClass.ordinals != null) {
                        for (int i = 1; i <= thisClass.ordinals.size(); i++) {
                            String name = thisClass.ordinals.get(i);
                            if (!skipKey || !isKeyField(name)) {
                                index = thisClass.setValueByField(
                                    name,
                                    objectVersion,
                                    recordVersion,
                                    null,
                                    index,
                                    list,
                                    valueMap);
                            }
                        }
                    }
                    for (String name : thisClass.values.keySet()) {
                        if (thisClass.fieldsWithOrdinals == null || !thisClass.fieldsWithOrdinals.contains(name)) {
                            if (!skipKey || !isKeyField(name)) {
                                index = thisClass.setValueByField(
                                    name,
                                    objectVersion,
                                    recordVersion,
                                    null,
                                    index,
                                    list,
                                    valueMap);
                            }
                        }
                    }
                    if (result == null) {
                        result = (T) thisClass.constructAndHydrateFromJavaMap(valueMap);
                    } else {
                        for (String field : valueMap.keySet()) {
                            ValueType value = this.values.get(field);
                            value.set(result, valueMap.get(field));
                        }
                    }
                    valueMap.clear();
                    thisClass = thisClass.superClazz;
                }
            }
            return result;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private T constructAndHydrate(Record record, Map<String, Object> map) {
        Map<String, Object> valueMap = new HashMap<>();
        try {
            ClassCacheEntry<?> thisClass = this;

            // If the object saved in the list was a subclass of the declared type,
            // it must have the type name in the map
            // Note that there is a performance implication of using subclasses.
            String className = map == null ? record.getString(TYPE_NAME) : (String) map.get(TYPE_NAME);
            if (className != null) {
                thisClass = ClassCache.getInstance().getCacheEntryFromStoredName(className);
                if (thisClass == null) {
                    Class<?> typeClazz = Class.forName(className);
                    thisClass = ClassCache.getInstance().loadClass(typeClazz, this.mapper);
                }
            }

            T result = null;
            while (thisClass != null) {
                for (String name : thisClass.values.keySet()) {
                    ValueType value = thisClass.values.get(name);
                    Object dingoValue = record == null ? map.get(name) : record.getValue(name.toUpperCase());
                    valueMap.put(name, value.getTypeMapper().fromDingoFormat(dingoValue));
                }
                if (result == null) {
                    result = (T) thisClass.constructAndHydrateFromJavaMap(valueMap);
                } else {
                    for (String field : valueMap.keySet()) {
                        ValueType value = thisClass.values.get(field);
                        value.set(result, valueMap.get(field));
                    }
                }
                valueMap.clear();
                thisClass = thisClass.superClazz;
            }

            return result;
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    public void hydrateFromRecord(Record record, Object instance) {
        this.hydrateFromRecordOrMap(record, null, instance);
    }

    public void hydrateFromMap(Map<String, Object> map, Object instance) {
        this.hydrateFromRecordOrMap(null, map, instance);
    }

    private void hydrateFromRecordOrMap(Record record, Map<String, Object> map, Object instance) {
        try {
            ClassCacheEntry<?> thisClass = this;
            while (thisClass != null) {
                for (String name : this.values.keySet()) {
                    ValueType value = this.values.get(name);
                    Object dingoValue = record == null ? map.get(name) : record.getValue(name);
                    value.set(instance, value.getTypeMapper().fromDingoFormat(dingoValue));
                }
                thisClass = thisClass.superClazz;
            }
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    private int setValueByField(String name, int objectVersion, int recordVersion, Object instance, int index,
                                List<Object> list, Map<String, Object> map) throws ReflectiveOperationException {
        ValueType value = this.values.get(name);
        TypeMapper typeMapper = value.getTypeMapper();

        Object dingoValue = list.get(index++);
        Object javaValue = dingoValue == null ? null : typeMapper.fromDingoFormat(dingoValue);
        if (instance == null) {
            map.put(name, javaValue);
        } else {
            value.set(instance, javaValue);
        }
        return index;
    }

    public void hydrateFromList(List<Object> list, Object instance) {
        this.hydrateFromList(list, instance, false);
    }

    public void hydrateFromList(List<Object> list, Object instance, boolean skipKey) {
        try {
            int index = 0;
            int endIndex = list.size();
            ClassCacheEntry<?> thisClass = this;
            while (thisClass != null) {
                if (index < endIndex) {
                    Object lastValue = list.get(endIndex - 1);
                    int recordVersion = 1;
                    if ((lastValue instanceof String) && (((String) lastValue).startsWith(VERSION_PREFIX))) {
                        recordVersion = Integer.parseInt(((String) lastValue).substring(2));
                        endIndex--;
                    }
                    int objectVersion = thisClass.version;
                    if (ordinals != null) {
                        for (int i = 1; i <= ordinals.size(); i++) {
                            String name = ordinals.get(i);
                            if (!skipKey || !isKeyField(name)) {
                                index = setValueByField(
                                    name,
                                    objectVersion,
                                    recordVersion,
                                    instance,
                                    index,
                                    list,
                                    null);
                            }
                        }
                    }
                    for (String name : this.values.keySet()) {
                        if (this.fieldsWithOrdinals == null || !thisClass.fieldsWithOrdinals.contains(name)) {
                            if (!skipKey || !isKeyField(name)) {
                                index = setValueByField(
                                    name,
                                    objectVersion,
                                    recordVersion,
                                    instance,
                                    index,
                                    list,
                                    null);
                            }
                        }
                    }
                    thisClass = thisClass.superClazz;
                }
            }
        } catch (ReflectiveOperationException ref) {
            throw new DingoClientException(ref.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private T constructAndHydrateFromJavaMap(Map<String, Object> javaValuesMap) throws ReflectiveOperationException {
        // Now form the values which satisfy the constructor
        T result;
        if (factoryConstructorMethod != null) {
            Object[] args;
            switch (factoryConstructorType) {
                case CLASS:
                    args = new Object[]{this.clazz};
                    break;
                case MAP:
                    args = new Object[]{javaValuesMap};
                    break;
                case CLASS_MAP:
                    args = new Object[]{this.clazz, javaValuesMap};
                    break;
                default:
                    args = null;
            }
            result = (T) factoryConstructorMethod.invoke(null, args);
        } else {
            Object[] args = new Object[constructorParamBins.length];
            for (int i = 0; i < constructorParamBins.length; i++) {
                if (javaValuesMap.containsKey(constructorParamBins[i])) {
                    args[i] = javaValuesMap.get(constructorParamBins[i]);
                } else {
                    args[i] = constructorParamDefaults[i];
                }
                javaValuesMap.remove(constructorParamBins[i]);
            }
            result = constructor.newInstance(args);
        }
        // Once the object has been created, we need to store it against the current key so that
        // recursive objects resolve correctly
        LoadedObjectResolver.setObjectForCurrentKey(result);

        for (String field : javaValuesMap.keySet()) {
            ValueType value = this.values.get(field);
            Object object = javaValuesMap.get(field);
            if (object == null && value.getType().isPrimitive()) {
                object = PrimitiveDefaults.getDefaultValue(value.getType());
            }
            value.set(result, object);
        }
        return result;
    }


    public ValueType getValueFromColumnName(String name) {
        return this.values.get(name);
    }

    @Override
    public String toString() {
        String result = String.format("ClassCacheEntry<%s> (database=%s,table=%s,subclass=%b,shortName=%s)",
            this.getUnderlyingClass().getSimpleName(),
            this.database,
            this.tableName,
            this.isChildClass,
            this.shortenedClassName);
        return result;
    }
}
