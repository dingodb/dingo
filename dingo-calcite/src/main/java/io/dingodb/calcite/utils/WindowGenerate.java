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

package io.dingodb.calcite.utils;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.meta.DingoSortedMultiMap;
import io.dingodb.common.util.Pair;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.calcite.adapter.enumerable.EnumUtils.generateCollatorExpression;

public class WindowGenerate {
    public static Method SORTED_MULTI_MAP_SINGLETON;
    public static Method SORTED_MULTI_MAP_ARRAYS;
    public static Method SORTED_MULTI_MAP_PUT_MULTI;
    static {
        SORTED_MULTI_MAP_SINGLETON = (Method)Types.lookupMethod(DingoSortedMultiMap.class, "singletonArrayIterator", new Class[]{Comparator.class, List.class});
        SORTED_MULTI_MAP_ARRAYS = (Method)Types.lookupMethod(DingoSortedMultiMap.class, "arrays", new Class[]{Comparator.class});
        SORTED_MULTI_MAP_PUT_MULTI = (Method)Types.lookupMethod(DingoSortedMultiMap.class, "putMulti", new Class[]{Object.class, Object.class});
    }

    public static Expression generateComparator(RelCollation collation, RelDataType rowType) {
        BlockBuilder body = new BlockBuilder();
        final ParameterExpression parameterV0 =
            Expressions.parameter(Object[].class, "v0");
        final ParameterExpression parameterV1 =
            Expressions.parameter(Object[].class, "v1");
        final ParameterExpression parameterC =
            Expressions.parameter(int.class, "c");
        final int mod =
            collation.getFieldCollations().size() == 1 ? Modifier.FINAL : 0;
        body.add(Expressions.declare(mod, parameterC, null));
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            final int index = fieldCollation.getFieldIndex();
            final RelDataType fieldType = rowType.getFieldList().get(index).getType();
            final Expression fieldComparator = generateCollatorExpression(fieldType.getCollation());

            Expression ix = Expressions.constant(index);
            Expression arg0 = Expressions.arrayIndex(parameterV0, ix);
            Expression arg1 = Expressions.arrayIndex(parameterV1, ix);

            arg0 = EnumUtils.convert(arg0, Comparable.class);
            arg1 = EnumUtils.convert(arg1, Comparable.class);
            //switch (Primitive.flavor(fieldClass(index))) {
            //    case OBJECT:
            //        arg0 = EnumUtils.convert(arg0, Comparable.class);
            //        arg1 = EnumUtils.convert(arg1, Comparable.class);
            //        break;
            //    default:
            //        break;
            //}
            final boolean nullsFirst =
                fieldCollation.nullDirection
                    == RelFieldCollation.NullDirection.FIRST;
            final boolean descending =
                fieldCollation.getDirection()
                    == RelFieldCollation.Direction.DESCENDING;
            body.add(
                Expressions.statement(
                    Expressions.assign(
                        parameterC,
                        Expressions.call(
                            Utilities.class,
                            fieldNullable(rowType, index)
                                ? (nullsFirst != descending
                                ? "compareNullsFirst"
                                : "compareNullsLast")
                                : "compare",
                            Expressions.list(
                                    arg0,
                                    arg1)
                                .appendIfNotNull(fieldComparator)))));
            body.add(
                Expressions.ifThen(
                    Expressions.notEqual(
                        parameterC, Expressions.constant(0)),
                    Expressions.return_(
                        null,
                        descending
                            ? Expressions.negate(parameterC)
                            : parameterC)));
        }
        body.add(
            Expressions.return_(null, Expressions.constant(0)));

        final List<MemberDeclaration> memberDeclarations =
            Expressions.list(
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    int.class,
                    "compare",
                    ImmutableList.of(parameterV0, parameterV1),
                    body.toBlock()));
        final ParameterExpression parameterO0 =
            Expressions.parameter(Object.class, "o0");
        final ParameterExpression parameterO1 =
            Expressions.parameter(Object.class, "o1");
        BlockBuilder bridgeBody = new BlockBuilder();
        bridgeBody.add(
            Expressions.return_(
                null,
                Expressions.call(
                    Expressions.parameter(
                        Comparable.class, "this"),
                    BuiltInMethod.COMPARATOR_COMPARE.method,
                    Expressions.convert_(
                        parameterO0,
                        Object[].class),
                    Expressions.convert_(
                        parameterO1,
                        Object[].class))));
        memberDeclarations.add(
            EnumUtils.overridingMethodDecl(
                BuiltInMethod.COMPARATOR_COMPARE.method,
                ImmutableList.of(parameterO0, parameterO1),
                bridgeBody.toBlock()));
        return Expressions.new_(
            Comparator.class,
            ImmutableList.of(),
            memberDeclarations);
    }

    public static Pair<Expression, Expression> getPartitionIterator(
        BlockBuilder builder, Expression source, Window.Group group,
        Expression comparator, PhysType inputPhysType
    ) {
        if (group.keys.isEmpty()) {
            // dec list
            final Expression tempList_ = builder.append("tempList", Expressions.new_(
                ArrayList.class));
            BlockBuilder builder2 = new BlockBuilder();
            ParameterExpression rows_ =
                (ParameterExpression) builder2.append(
                    "rows",
                    Expressions.convert_(
                        Expressions.call(
                            source, BuiltInMethod.ITERATOR_NEXT.method),
                        Object[].class),
                    false);
            builder2.add(
                Expressions.statement(
                    Expressions.call(
                        tempList_,
                        BuiltInMethod.COLLECTION_ADD.method,
                        rows_)));
            builder.add(Expressions.while_(
                Expressions.call(
                    source,
                    BuiltInMethod.ITERATOR_HAS_NEXT.method),
                builder2.toBlock()));

            // while source.hasNext -> list.add(source.next)
            return Pair.of(tempList_,
                builder.append(
                    "iterator",
                    Expressions.call(
                        null,
                        SORTED_MULTI_MAP_SINGLETON,
                        comparator,
                        tempList_)));
        }
        Expression multiMap_ =
            builder.append(
                "multiMap", Expressions.new_(DingoSortedMultiMap.class));
        final BlockBuilder builder2 = new BlockBuilder();

        ParameterExpression key;

        final ParameterExpression rows_ =
            (ParameterExpression) builder2.append(
                "rows",
                Expressions.convert_(
                    Expressions.call(
                        source, BuiltInMethod.ITERATOR_NEXT.method),
                    Object[].class),
                false);

        org.apache.calcite.util.Pair<Type, List<Expression>> selector =
            inputPhysType.selector(rows_, group.keys.asList(), JavaRowFormat.CUSTOM);
        if (selector.left instanceof Types.RecordType) {
            Types.RecordType keyJavaType = (Types.RecordType) selector.left;
            List<Expression> initExpressions = selector.right;
            key = Expressions.parameter(keyJavaType, "key");
            builder2.add(Expressions.declare(0, key, null));
            builder2.add(
                Expressions.statement(
                    Expressions.assign(key, Expressions.new_(keyJavaType))));
            List<Types.RecordField> fieldList = keyJavaType.getRecordFields();
            for (int i = 0; i < initExpressions.size(); i++) {
                Expression right = initExpressions.get(i);
                builder2.add(
                    Expressions.statement(
                        Expressions.assign(
                            Expressions.field(key, fieldList.get(i)), right)));
            }
        } else {
            DeclarationStatement declare =
                Expressions.declare(0, "key", selector.right.get(0));
            builder2.add(declare);
            key = declare.parameter;
        }

        builder2.add(
            Expressions.statement(
                Expressions.call(
                    multiMap_,
                    SORTED_MULTI_MAP_PUT_MULTI,
                    key,
                    rows_)));

        builder.add(Expressions.while_(
            Expressions.call(
                source,
                BuiltInMethod.ITERATOR_HAS_NEXT.method),
            builder2.toBlock()));

        return Pair.of(multiMap_,
            builder.append(
                "iterator",
                Expressions.call(
                    multiMap_,
                    SORTED_MULTI_MAP_ARRAYS,
                    comparator)));
    }

    public static boolean fieldNullable(RelDataType rowType, int field) {
        return rowType.getFieldList().get(field).getType().isNullable();
    }

}
