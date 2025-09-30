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

package io.dingodb.calcite.visitor.function;

import com.google.common.collect.ImmutableList;
import io.dingodb.calcite.rel.DingoWindow;
import io.dingodb.calcite.type.DingoSqlTypeFactory;
import io.dingodb.calcite.utils.DingoEnumUtils;
import io.dingodb.calcite.utils.DingoRelResult;
import io.dingodb.calcite.utils.WindowGenerate;
import io.dingodb.calcite.visitor.DingoJobVisitor;
import io.dingodb.common.Location;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.util.Pair;
import io.dingodb.exec.base.IdGenerator;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.dag.Edge;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.params.WindowFunctionParam;
import io.dingodb.exec.transaction.base.ITransaction;
import io.dingodb.tool.api.WindowService;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.AggImpState;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.WinAggAddContext;
import org.apache.calcite.adapter.enumerable.WinAggContext;
import org.apache.calcite.adapter.enumerable.WinAggFrameResultContext;
import org.apache.calcite.adapter.enumerable.WinAggImplementor;
import org.apache.calcite.adapter.enumerable.impl.WinAggAddContextImpl;
import org.apache.calcite.adapter.enumerable.impl.WinAggResetContextImpl;
import org.apache.calcite.adapter.enumerable.impl.WinAggResultContextImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.dingodb.calcite.rel.DingoRel.dingo;
import static io.dingodb.common.util.Utils.sole;
import static io.dingodb.exec.utils.OperatorCodeUtils.WINDOW_FUNCTION;
import static org.apache.calcite.adapter.enumerable.EnumerableRelImplementor.classDecl;

@Slf4j
public final class DingoWindowVisitFun {

    private DingoWindowVisitFun() {
    }

    public static @NonNull Collection<Vertex> visit(
        Job job,
        @NonNull IdGenerator idGenerator,
        Location currentLocation,
        DingoJobVisitor visitor,
        ITransaction transaction,
        @NonNull DingoWindow rel
    ) {
        Collection<Vertex> inputs = dingo(rel.getInput()).accept(visitor);

        WindowService windowService = generateCode(rel);
        WindowFunctionParam windowFunctionParam = new WindowFunctionParam(windowService);
        Vertex vertex = new Vertex(WINDOW_FUNCTION, windowFunctionParam);
        Vertex input = sole(inputs);
        Task task = input.getTask();
        vertex.setId(idGenerator.getOperatorId(task.getId()));
        task.putVertex(vertex);
        input.setPin(0);
        Edge edge = new Edge(input, vertex);
        input.addEdge(edge);
        vertex.addIn(edge);
        return ImmutableList.of(vertex);
    }

    public static WindowService generateCode(DingoWindow rel) {
        BlockBuilder builder = new BlockBuilder();
        ParameterExpression prevStart =
            Expressions.parameter(int.class, builder.newName("prevStart"));
        ParameterExpression prevEnd =
            Expressions.parameter(int.class, builder.newName("prevEnd"));
        Expression source =
            Expressions.parameter(0, Iterator.class, "paramSource");

        PhysType inputPhysType = PhysTypeImpl.of(DingoSqlTypeFactory.INSTANCE, rel.getInput().getRowType(), JavaRowFormat.ARRAY);

        DingoRelResult result = new DingoRelResult(null, inputPhysType, inputPhysType.getFormat());

        final List<Expression> translatedConstants =
            new ArrayList<>(rel.constants.size());
        for (RexLiteral constant : rel.constants) {
            translatedConstants.add(
                RexToLixTranslator.translateLiteral(constant, constant.getType(),
                    DingoSqlTypeFactory.INSTANCE, RexImpTable.NullAs.NULL));
        }

        builder.add(Expressions.declare(0, prevStart, null));
        builder.add(Expressions.declare(0, prevEnd, null));
        for (int windowIdx = 0; windowIdx < rel.groups.size(); windowIdx++) {
            Window.Group group = rel.groups.get(windowIdx);
            final Expression comparator_ =
                builder.append(
                    "comparator",
                    WindowGenerate.generateComparator(group.collation(), rel.getInput().getRowType()));
            Pair<Expression, Expression> partitionIterator =
                WindowGenerate.getPartitionIterator(builder, source, group, comparator_,
                inputPhysType);
            final Expression collectionExpr = partitionIterator.getKey();
            final Expression iterator_ = partitionIterator.getValue();

            List<AggImpState> aggs = new ArrayList<>();
            List<AggregateCall> aggregateCalls = group.getAggregateCalls(rel);
            for (int aggIdx = 0; aggIdx < aggregateCalls.size(); aggIdx++) {
                AggregateCall call = aggregateCalls.get(aggIdx);
                if (call.ignoreNulls()) {
                    throw new UnsupportedOperationException("IGNORE NULLS not supported");
                }
                aggs.add(new AggImpState(aggIdx, call, true));
            }

            // The output from this stage is the input plus the aggregate functions.
            JavaTypeFactory typeFactory = DingoSqlTypeFactory.INSTANCE;
            final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
            typeBuilder.addAll(inputPhysType.getRowType().getFieldList());
            for (AggImpState agg : aggs) {
                // CALCITE-4326
                String name = Objects.requireNonNull(agg.call.name,
                    () -> "agg.call.name for " + agg.call);
                typeBuilder.add(name, agg.call.type);
            }
            RelDataType outputRowType = typeBuilder.build();
            final PhysType outputPhysType =
                PhysTypeImpl.of(
                    typeFactory, outputRowType, JavaRowFormat.ARRAY);

            final Expression list_ =
                builder.append(
                    "list",
                    Expressions.new_(
                        ArrayList.class,
                        Expressions.call(
                            collectionExpr, BuiltInMethod.COLLECTION_SIZE.method)),
                    false);

            Pair<Expression, Expression> collationKey =
                getRowCollationKey(builder, inputPhysType, group, windowIdx);
            Expression keySelector = collationKey.getKey();
            Expression keyComparator = collationKey.getValue();
            final BlockBuilder builder3 = new BlockBuilder();
            final Expression rows_ =
                builder3.append(
                    "rows",
                    Expressions.convert_(
                        Expressions.call(
                            iterator_, BuiltInMethod.ITERATOR_NEXT.method),
                        Object[].class),
                    false);

            builder3.add(
                Expressions.statement(
                    Expressions.assign(prevStart, Expressions.constant(-1))));
            builder3.add(
                Expressions.statement(
                    Expressions.assign(prevEnd,
                        Expressions.constant(Integer.MAX_VALUE))));

            final BlockBuilder builder4 = new BlockBuilder();

            final ParameterExpression i_ =
                Expressions.parameter(int.class, builder4.newName("i"));

            //Expression ix = Expressions.constant(0);
            //final Expression tmp = Expressions.arrayIndex(rows_, ix);
            final Expression row_ =
                builder4.append(
                    "row",
                    EnumUtils.convert(
                        Expressions.arrayIndex(rows_, i_),
                        Object[].class));

            final RexToLixTranslator.InputGetter inputGetter =
                new DingoWindow.WindowRelInputGetter(row_, inputPhysType,
                    result.physType.getRowType().getFieldCount(),
                    translatedConstants);

            final RexToLixTranslator translator =
                RexToLixTranslator.forAggregation(typeFactory, builder4,
                    inputGetter, SqlConformanceEnum.DEFAULT);

            final List<Expression> outputRow = new ArrayList<>();
            int fieldCountWithAggResults =
                inputPhysType.getRowType().getFieldCount();
            for (int i = 0; i < fieldCountWithAggResults; i++) {
                outputRow.add(
                    inputPhysType.fieldReference(
                        row_, i,
                        outputPhysType.getJavaFieldType(i)));
            }

            declareAndResetState(typeFactory, builder, result, windowIdx, aggs,
                outputPhysType, outputRow, rel);

            // There are assumptions that minX==0. If ever change this, look for
            // frameRowCount, bounds checking, etc
            final Expression minX = Expressions.constant(0);
            final Expression partitionRowCount =
                builder3.append("partRows", Expressions.field(rows_, "length"));
            final Expression maxX = builder3.append("maxX",
                Expressions.subtract(
                    partitionRowCount, Expressions.constant(1)));

            final Expression startUnchecked = builder4.append("start",
                translateBound(translator, i_, row_, minX, maxX, rows_,
                    group, true, inputPhysType, keySelector, keyComparator));
            final Expression endUnchecked = builder4.append("end",
                translateBound(translator, i_, row_, minX, maxX, rows_,
                    group, false, inputPhysType, keySelector, keyComparator));

            final Expression startX;
            final Expression endX;
            final Expression hasRows;
            if (group.isAlwaysNonEmpty()) {
                startX = startUnchecked;
                endX = endUnchecked;
                hasRows = Expressions.constant(true);
            } else {
                Expression startTmp =
                    group.lowerBound.isUnbounded() || startUnchecked == i_
                        ? startUnchecked
                        : builder4.append("startTmp",
                        Expressions.call(null, BuiltInMethod.MATH_MAX.method,
                            startUnchecked, minX));
                Expression endTmp =
                    group.upperBound.isUnbounded() || endUnchecked == i_
                        ? endUnchecked
                        : builder4.append("endTmp",
                        Expressions.call(null, BuiltInMethod.MATH_MIN.method,
                            endUnchecked, maxX));

                ParameterExpression startPe = Expressions.parameter(0, int.class,
                    builder4.newName("startChecked"));
                ParameterExpression endPe = Expressions.parameter(0, int.class,
                    builder4.newName("endChecked"));
                builder4.add(Expressions.declare(Modifier.FINAL, startPe, null));
                builder4.add(Expressions.declare(Modifier.FINAL, endPe, null));

                hasRows = builder4.append("hasRows",
                    Expressions.lessThanOrEqual(startTmp, endTmp));
                builder4.add(
                    Expressions.ifThenElse(hasRows,
                        Expressions.block(
                            Expressions.statement(
                                Expressions.assign(startPe, startTmp)),
                            Expressions.statement(
                                Expressions.assign(endPe, endTmp))),
                        Expressions.block(
                            Expressions.statement(
                                Expressions.assign(startPe, Expressions.constant(-1))),
                            Expressions.statement(
                                Expressions.assign(endPe, Expressions.constant(-1))))));
                startX = startPe;
                endX = endPe;
            }

            final BlockBuilder builder5 = new BlockBuilder(true, builder4);

            BinaryExpression rowCountWhenNonEmpty = Expressions.add(
                startX == minX ? endX : Expressions.subtract(endX, startX),
                Expressions.constant(1));

            final Expression frameRowCount;

            if (hasRows.equals(Expressions.constant(true))) {
                frameRowCount =
                    builder4.append("totalRows", rowCountWhenNonEmpty);
            } else {
                frameRowCount =
                    builder4.append("totalRows",
                        Expressions.condition(hasRows, rowCountWhenNonEmpty,
                            Expressions.constant(0)));
            }

            ParameterExpression actualStart = Expressions.parameter(
                0, int.class, builder5.newName("actualStart"));

            final BlockBuilder builder6 = new BlockBuilder(true, builder5);
            builder6.add(
                Expressions.statement(Expressions.assign(actualStart, startX)));

            for (final AggImpState agg : aggs) {
                List<Expression> aggState = Objects.requireNonNull(agg.state, "agg.state");
                agg.implementor.implementReset(Objects.requireNonNull(agg.context, "agg.context"),
                    new WinAggResetContextImpl(builder6, aggState, i_, startX, endX,
                        hasRows, frameRowCount, partitionRowCount));
            }

            Expression lowerBoundCanChange =
                group.lowerBound.isUnbounded() && group.lowerBound.isPreceding()
                    ? Expressions.constant(false)
                    : Expressions.notEqual(startX, prevStart);
            Expression needRecomputeWindow = Expressions.orElse(
                lowerBoundCanChange,
                Expressions.lessThan(endX, prevEnd));

            BlockStatement resetWindowState = builder6.toBlock();
            if (resetWindowState.statements.size() == 1) {
                builder5.add(
                    Expressions.declare(0, actualStart,
                        Expressions.condition(needRecomputeWindow, startX,
                            Expressions.add(prevEnd, Expressions.constant(1)))));
            } else {
                builder5.add(
                    Expressions.declare(0, actualStart, null));
                builder5.add(
                    Expressions.ifThenElse(needRecomputeWindow,
                        resetWindowState,
                        Expressions.statement(
                            Expressions.assign(actualStart,
                                Expressions.add(prevEnd, Expressions.constant(1))))));
            }

            if (lowerBoundCanChange instanceof BinaryExpression) {
                builder5.add(
                    Expressions.statement(Expressions.assign(prevStart, startX)));
            }
            builder5.add(
                Expressions.statement(Expressions.assign(prevEnd, endX)));

            final BlockBuilder builder7 = new BlockBuilder(true, builder5);
            final DeclarationStatement jDecl =
                Expressions.declare(0, "j", actualStart);

            final PhysType inputPhysTypeFinal = inputPhysType;
            final Function<BlockBuilder, WinAggFrameResultContext>
                resultContextBuilder =
                getBlockBuilderWinAggFrameResultContextFunction(typeFactory,
                    SqlConformanceEnum.DEFAULT, result, translatedConstants,
                    comparator_, rows_, i_, startX, endX, minX, maxX,
                    hasRows, frameRowCount, partitionRowCount,
                    jDecl, inputPhysTypeFinal);

            final Function<AggImpState, List<RexNode>> rexArguments = agg -> {
                List<Integer> argList = agg.call.getArgList();
                List<RelDataType> inputTypes =
                    DingoEnumUtils.fieldRowTypes(
                        result.physType.getRowType(),
                        rel.constants,
                        argList);
                List<RexNode> args = new ArrayList<>(inputTypes.size());
                for (int i = 0; i < argList.size(); i++) {
                    Integer idx = argList.get(i);
                    args.add(new RexInputRef(idx, inputTypes.get(i)));
                }
                return args;
            };

            implementAdd(aggs, builder7, resultContextBuilder, rexArguments, jDecl);

            BlockStatement forBlock = builder7.toBlock();
            if (!forBlock.statements.isEmpty()) {
                // For instance, row_number does not use for loop to compute the value
                Statement forAggLoop = Expressions.for_(
                    Arrays.asList(jDecl),
                    Expressions.lessThanOrEqual(jDecl.parameter, endX),
                    Expressions.preIncrementAssign(jDecl.parameter),
                    forBlock);
                if (!hasRows.equals(Expressions.constant(true))) {
                    forAggLoop = Expressions.ifThen(hasRows, forAggLoop);
                }
                builder5.add(forAggLoop);
            }

            if (implementResult(aggs, builder5, resultContextBuilder, rexArguments,
                true)) {
                builder4.add(
                    Expressions.ifThen(
                        Expressions.orElse(lowerBoundCanChange,
                            Expressions.notEqual(endX, prevEnd)),
                        builder5.toBlock()));
            }

            implementResult(aggs, builder4, resultContextBuilder, rexArguments,
                false);

            // list.add(new Object[]{xx,xx,xx});
            builder4.add(
                Expressions.statement(
                    Expressions.call(
                        list_,
                        BuiltInMethod.COLLECTION_ADD.method,
                        outputPhysType.record(outputRow))));

            // for (int i = 0; i < rows.length; i++) {
            //  build4.body
            // }
            builder3.add(
                Expressions.for_(
                    Expressions.declare(0, i_, Expressions.constant(0)),
                    Expressions.lessThan(
                        i_,
                        Expressions.field(rows_, "length")),
                    Expressions.preIncrementAssign(i_),
                    builder4.toBlock()));
            // while(iterator.hasNext()) {
            // builder3.body
            // }
            builder.add(
                Expressions.while_(
                    Expressions.call(
                        iterator_,
                        BuiltInMethod.ITERATOR_HAS_NEXT.method),
                    builder3.toBlock()));
            builder.add(
                Expressions.statement(
                    Expressions.call(
                        collectionExpr,
                        BuiltInMethod.MAP_CLEAR.method)));

            // We're not assigning to "source". For each group, create a new
            // final variable called "source" or "sourceN".
            try {
                Method method = List.class.getDeclaredMethod("iterator");
                source =
                    builder.append(
                        "source",
                        Expressions.call(list_,
                            method));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
            LogUtils.info(log, "outputPhysType field count:{}", outputPhysType.getRowType().getFieldCount());
            inputPhysType = outputPhysType;
        }
        builder.add(Expressions.return_(null, source));
        try {
            return generateInterface(builder.toBlock());
        } catch (InvocationTargetException | ClassNotFoundException
            | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static WindowService generateInterface(BlockStatement methodBody)
        throws ClassNotFoundException, InvocationTargetException,
         InstantiationException, IllegalAccessException {
        List<MemberDeclaration> memberDeclarations = new ArrayList<>();
        TypeRegistrar typeRegistrar = new TypeRegistrar(memberDeclarations);
        typeRegistrar.go(methodBody);
        ParameterExpression source =
            Expressions.parameter(Iterator.class, "paramSource");
        memberDeclarations.add(Expressions.methodDecl(
            Modifier.PUBLIC,
            Iterator.class,
            "transform",
            Expressions.list(source),
            methodBody));
        ClassDeclaration classDeclaration = Expressions.classDecl(Modifier.PUBLIC,
            "DingoWindowService",
            null,
            Collections.singletonList(WindowService.class),
            memberDeclarations);
        String body = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);
        return generateCompileInterface(body);
    }


    public static WindowService generateCompileInterface(String classBody)
        throws ClassNotFoundException, InvocationTargetException,
        InstantiationException, IllegalAccessException {
        ICompilerFactory compilerFactory;
        ClassLoader classLoader =
            Objects.requireNonNull(DingoWindowVisitFun.class.getClassLoader(), "classLoader");
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory(classLoader);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Unable to instantiate java compiler", e);
        }
        final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
        compiler.setParentClassLoader(classLoader);
        String name = "WindowFunction";
        final String s = "public final class " + name + " implements "
            +  WindowService.class.getCanonicalName()
            + " {\n"
            + classBody
            + "\n"
            + "}";
        try {
            LogUtils.info(log, "-->" + s);
            compiler.cook(s);
        } catch (CompileException e) {
            throw new RuntimeException(e);
        }
        return (WindowService) compiler.getClassLoader().loadClass(name)
            .getDeclaredConstructors()[0].newInstance();
    }

    private static Pair<Expression, Expression> getRowCollationKey(
        BlockBuilder builder, PhysType inputPhysType,
        Window.Group group, int windowIdx) {
        if (!(group.isRows
            || (group.upperBound.isUnbounded() && group.lowerBound.isUnbounded()))) {
            org.apache.calcite.util.Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(
                    group.collation().getFieldCollations());
            // optimize=false to prevent inlining of object create into for-loops
            return Pair.of(
                builder.append("keySelector" + windowIdx, pair.getKey(), false),
                builder.append("keyComparator" + windowIdx, pair.getValue(), false));
        } else {
            return Pair.of(null, null);
        }
    }

    private static Expression translateBound(RexToLixTranslator translator,
                                             ParameterExpression i_, Expression row_, Expression min_, Expression max_,
                                             Expression rows_, Window.Group group, boolean lower, PhysType physType,
                                             Expression keySelector, Expression keyComparator) {
        // for example
        // final int end = org.apache.calcite.runtime.BinarySearch
        //    .upperBound(_rows, row.empid, i, _rows.length - 1, _keySelector0, _keyComparator0);
        RexWindowBound bound = lower ? group.lowerBound : group.upperBound;
        if (bound.isUnbounded()) {
            return bound.isPreceding() ? min_ : max_;
        }
        if (group.isRows) {
            if (bound.isCurrentRow()) {
                return i_;
            }
            RexNode node = bound.getOffset();
            Expression offs = translator.translate(node);;

            // Floating offset does not make sense since we refer to array index.
            // Nulls do not make sense as well.
            offs = EnumUtils.convert(offs, int.class);

            Expression b = i_;
            if (bound.isFollowing()) {
                b = Expressions.add(b, offs);
            } else {
                b = Expressions.subtract(b, offs);
            }
            return b;
        }
        Expression searchLower = min_;
        Expression searchUpper = max_;
        if (bound.isCurrentRow()) {
            if (lower) {
                searchUpper = i_;
            } else {
                searchLower = i_;
            }
        }

        List<RelFieldCollation> fieldCollations =
            group.collation().getFieldCollations();
        if (bound.isCurrentRow() && fieldCollations.size() != 1) {
            return Expressions.call(
                (lower
                    ? BuiltInMethod.BINARY_SEARCH5_LOWER
                    : BuiltInMethod.BINARY_SEARCH5_UPPER).method,
                rows_, row_, searchLower, searchUpper,
                Objects.requireNonNull(keySelector, "keySelector"),
                Objects.requireNonNull(keyComparator, "keyComparator"));
        }
        assert fieldCollations.size() == 1
            : "When using range window specification, ORDER BY should have"
            + " exactly one expression."
            + " Actual collation is " + group.collation();
        // isRange
        int orderKey =
            fieldCollations.get(0).getFieldIndex();
        RelDataType keyType =
            physType.getRowType().getFieldList().get(orderKey).getType();
        Type desiredKeyType = DingoSqlTypeFactory.INSTANCE.getJavaClass(keyType);
        if (bound.getOffset() == null) {
            desiredKeyType = Primitive.box(desiredKeyType);
        }
        Expression val = translator.translate(new RexInputRef(orderKey, keyType), desiredKeyType);;

        if (!bound.isCurrentRow()) {
            RexNode node = bound.getOffset();
            Expression offs = translator.translate(node);

            // TODO: support date + interval somehow
            // for dingo start
            Expression val1;
            if ("java.sql.Date".equalsIgnoreCase(desiredKeyType.getTypeName())) {
                if (val instanceof ParameterExpression && !"java.sql.Date".equalsIgnoreCase(val.getType().getTypeName())) {
                    val1 = Expressions.convert_(
                        val,
                        java.sql.Date.class);
                    Expression timestamp = Expressions.call(val1, "getTime");
                    Expression newDate;
                    if (bound.isFollowing()) {
                        Expression newTimestamp = Expressions.add(timestamp, offs);
                        newDate = Expressions.new_(java.sql.Date.class, newTimestamp);
                    } else {
                        Expression newTimestamp = Expressions.subtract(timestamp, offs);
                        newDate = Expressions.new_(java.sql.Date.class, newTimestamp);
                    }
                    Expression paramDate = Expressions.condition(Expressions.equal(val, Expressions.constant(null)), Expressions.constant(null), newDate);
                    return Expressions.call(
                        (lower
                            ? BuiltInMethod.BINARY_SEARCH6_LOWER
                            : BuiltInMethod.BINARY_SEARCH6_UPPER).method,
                        rows_, paramDate, searchLower, searchUpper,
                        Objects.requireNonNull(keySelector, "keySelector"),
                        Objects.requireNonNull(keyComparator, "keyComparator"));
                }
            } else if ("java.sql.Timestamp".equalsIgnoreCase(desiredKeyType.getTypeName())) {
                if (val instanceof ParameterExpression && !"java.sql.Timestamp".equalsIgnoreCase(val.getType().getTypeName())) {
                    val1 = Expressions.convert_(
                        val,
                        java.sql.Timestamp.class);
                    Expression timestamp = Expressions.call(val1, "getTime");
                    Expression newDate;
                    if (bound.isFollowing()) {
                        Expression newTimestamp = Expressions.add(timestamp, offs);
                        newDate = Expressions.new_(java.sql.Timestamp.class, newTimestamp);
                    } else {
                        Expression newTimestamp = Expressions.subtract(timestamp, offs);
                        newDate = Expressions.new_(java.sql.Timestamp.class, newTimestamp);
                    }
                    Expression paramDate = Expressions.condition(Expressions.equal(val, Expressions.constant(null)), Expressions.constant(null), newDate);
                    return Expressions.call(
                        (lower
                            ? BuiltInMethod.BINARY_SEARCH6_LOWER
                            : BuiltInMethod.BINARY_SEARCH6_UPPER).method,
                        rows_, paramDate, searchLower, searchUpper,
                        Objects.requireNonNull(keySelector, "keySelector"),
                        Objects.requireNonNull(keyComparator, "keyComparator"));
                }
            } else {
                if (bound.isFollowing()) {
                    val = Expressions.add(val, offs);
                } else {
                    val = Expressions.subtract(val, offs);
                }
            }
        }
        return Expressions.call(
            (lower
                ? BuiltInMethod.BINARY_SEARCH6_LOWER
                : BuiltInMethod.BINARY_SEARCH6_UPPER).method,
            rows_, val, searchLower, searchUpper,
            Objects.requireNonNull(keySelector, "keySelector"),
            Objects.requireNonNull(keyComparator, "keyComparator"));
    }

    private static boolean implementResult(List<AggImpState> aggs,
                                           final BlockBuilder builder,
                                           final Function<BlockBuilder, WinAggFrameResultContext> frame,
                                           final Function<AggImpState, List<RexNode>> rexArguments,
                                           boolean cachedBlock) {
        boolean nonEmpty = false;
        for (final AggImpState agg : aggs) {
            boolean needCache = true;
            if (agg.implementor instanceof WinAggImplementor) {
                WinAggImplementor imp = (WinAggImplementor) agg.implementor;
                needCache = imp.needCacheWhenFrameIntact();
            }
            if (needCache ^ cachedBlock) {
                // Regular aggregates do not change when the windowing frame keeps
                // the same. Ths
                continue;
            }
            nonEmpty = true;
            Expression res = agg.implementor.implementResult(Objects.requireNonNull(agg.context, "agg.context"),
                new WinAggResultContextImpl(builder, Objects.requireNonNull(agg.state, "agg.state"), frame) {
                    @Override public List<RexNode> rexArguments() {
                        return rexArguments.apply(agg);
                    }
                });
            // Several count(a) and count(b) might share the result
            Expression result = Objects.requireNonNull(agg.result,
                () -> "agg.result for " + agg.call);
            Expression aggRes = builder.append("a" + agg.aggIdx + "res",
                EnumUtils.convert(res, result.getType()));
            builder.add(
                Expressions.statement(Expressions.assign(result, aggRes)));
        }
        return nonEmpty;
    }

    private static void implementAdd(List<AggImpState> aggs,
                                     final BlockBuilder builder7,
                                     final Function<BlockBuilder, WinAggFrameResultContext> frame,
                                     final Function<AggImpState, List<RexNode>> rexArguments,
                                     final DeclarationStatement jDecl) {
        for (final AggImpState agg : aggs) {
            final WinAggAddContext addContext =
                new WinAggAddContextImpl(builder7, Objects.requireNonNull(agg.state, "agg.state"), frame) {
                    @Override public Expression currentPosition() {
                        return jDecl.parameter;
                    }

                    @Override public List<RexNode> rexArguments() {
                        return rexArguments.apply(agg);
                    }

                    @Override public RexNode rexFilterArgument() {
                        return null; // REVIEW
                    }
                };
            agg.implementor.implementAdd(Objects.requireNonNull(agg.context, "agg.context"), addContext);
        }
    }

    private static Function<BlockBuilder, WinAggFrameResultContext>
    getBlockBuilderWinAggFrameResultContextFunction(
        final JavaTypeFactory typeFactory, final SqlConformance conformance,
        final DingoRelResult result, final List<Expression> translatedConstants,
        final Expression comparator_,
        final Expression rows_, final ParameterExpression i_,
        final Expression startX, final Expression endX,
        final Expression minX, final Expression maxX,
        final Expression hasRows, final Expression frameRowCount,
        final Expression partitionRowCount,
        final DeclarationStatement jDecl,
        final PhysType inputPhysType) {
        return block -> new WinAggFrameResultContext() {
            @Override public RexToLixTranslator rowTranslator(Expression rowIndex) {
                Expression row =
                    getRow(rowIndex);
                final RexToLixTranslator.InputGetter inputGetter =
                    new DingoWindow.WindowRelInputGetter(row, inputPhysType,
                        result.physType.getRowType().getFieldCount(),
                        translatedConstants);

                return RexToLixTranslator.forAggregation(typeFactory,
                    block, inputGetter, conformance);
            }

            @Override public Expression computeIndex(Expression offset,
                                                     WinAggImplementor.SeekType seekType) {
                Expression index;
                if (seekType == WinAggImplementor.SeekType.AGG_INDEX) {
                    index = jDecl.parameter;
                } else if (seekType == WinAggImplementor.SeekType.SET) {
                    index = i_;
                } else if (seekType == WinAggImplementor.SeekType.START) {
                    index = startX;
                } else if (seekType == WinAggImplementor.SeekType.END) {
                    index = endX;
                } else {
                    throw new IllegalArgumentException("SeekSet " + seekType
                        + " is not supported");
                }
                if (!Expressions.constant(0).equals(offset)) {
                    index = block.append("idx", Expressions.add(index, offset));
                }
                return index;
            }

            private Expression checkBounds(Expression rowIndex,
                                           Expression minIndex, Expression maxIndex) {
                if (rowIndex == i_ || rowIndex == startX || rowIndex == endX) {
                    // No additional bounds check required
                    return hasRows;
                }

                //noinspection UnnecessaryLocalVariable
                Expression res = block.append("rowInFrame",
                    Expressions.foldAnd(
                        ImmutableList.of(hasRows,
                            Expressions.greaterThanOrEqual(rowIndex, minIndex),
                            Expressions.lessThanOrEqual(rowIndex, maxIndex))));

                return res;
            }

            @Override public Expression rowInFrame(Expression rowIndex) {
                return checkBounds(rowIndex, startX, endX);
            }

            @Override public Expression rowInPartition(Expression rowIndex) {
                return checkBounds(rowIndex, minX, maxX);
            }

            @Override public Expression compareRows(Expression a, Expression b) {
                return Expressions.call(comparator_,
                    BuiltInMethod.COMPARATOR_COMPARE.method,
                    getRow(a), getRow(b));
            }

            public Expression getRow(Expression rowIndex) {
                return block.append(
                    "jRow",
                    EnumUtils.convert(
                        Expressions.arrayIndex(rows_, rowIndex),
                        inputPhysType.getJavaRowType()));
            }

            @Override public Expression index() {
                return i_;
            }

            @Override public Expression startIndex() {
                return startX;
            }

            @Override public Expression endIndex() {
                return endX;
            }

            @Override public Expression hasRows() {
                return hasRows;
            }

            @Override public Expression getFrameRowCount() {
                return frameRowCount;
            }

            @Override public Expression getPartitionRowCount() {
                return partitionRowCount;
            }
        };
    }

    private static void declareAndResetState(final JavaTypeFactory typeFactory,
                                      BlockBuilder builder, final DingoRelResult result, int windowIdx,
                                      List<AggImpState> aggs, PhysType outputPhysType,
                                      List<Expression> outputRow, DingoWindow rel) {
        for (final AggImpState agg : aggs) {
            agg.context =
                new WinAggContext() {
                    @Override public SqlAggFunction aggregation() {
                        return agg.call.getAggregation();
                    }

                    @Override public RelDataType returnRelType() {
                        return agg.call.type;
                    }

                    @Override public Type returnType() {
                        return DingoEnumUtils.javaClass(typeFactory, returnRelType());
                    }

                    @Override public List<? extends Type> parameterTypes() {
                        return DingoEnumUtils.fieldTypes(typeFactory,
                            parameterRelTypes());
                    }

                    @Override public List<? extends RelDataType> parameterRelTypes() {
                        return DingoEnumUtils.fieldRowTypes(result.physType.getRowType(),
                            rel.constants, agg.call.getArgList());
                    }

                    @Override public List<ImmutableBitSet> groupSets() {
                        throw new UnsupportedOperationException();
                    }

                    @Override public List<Integer> keyOrdinals() {
                        throw new UnsupportedOperationException();
                    }

                    @Override public List<? extends RelDataType> keyRelTypes() {
                        throw new UnsupportedOperationException();
                    }

                    @Override public List<? extends Type> keyTypes() {
                        throw new UnsupportedOperationException();
                    }
                };
            String aggName = "a" + agg.aggIdx;
            if (CalciteSystemProperty.DEBUG.value()) {
                aggName = Util.toJavaId(agg.call.getAggregation().getName(), 0)
                    .substring("ID$0$".length()) + aggName;
            }
            List<Type> state = agg.implementor.getStateType(agg.context);
            final List<Expression> decls = new ArrayList<>(state.size());
            for (int i = 0; i < state.size(); i++) {
                Type type = state.get(i);
                ParameterExpression pe =
                    Expressions.parameter(type,
                        builder.newName(aggName
                            + "s" + i + "w" + windowIdx));
                builder.add(Expressions.declare(0, pe, null));
                decls.add(pe);
            }
            agg.state = decls;
            Type aggHolderType = agg.context.returnType();
            Type aggStorageType =
                outputPhysType.getJavaFieldType(outputRow.size());
            if (Primitive.is(aggHolderType) && !Primitive.is(aggStorageType)) {
                aggHolderType = Primitive.box(aggHolderType);
            }
            ParameterExpression aggRes = Expressions.parameter(0,
                aggHolderType,
                builder.newName(aggName + "w" + windowIdx));

            builder.add(
                Expressions.declare(0, aggRes,
                    Expressions.constant(
                        Optional.ofNullable(Primitive.of(aggRes.getType()))
                            .map(x -> x.defaultValue)
                            .orElse(null),
                        aggRes.getType())));
            agg.result = aggRes;
            outputRow.add(aggRes);
            agg.implementor.implementReset(agg.context,
                new WinAggResetContextImpl(builder, agg.state,
                    null, null, null, null,
                    null, null));
        }
    }

    public static class TypeRegistrar {
        private final List<MemberDeclaration> memberDeclarations;
        private final Set<Type> seen = new HashSet<>();

        TypeRegistrar(List<MemberDeclaration> memberDeclarations) {
            this.memberDeclarations = memberDeclarations;
        }

        private void register(Type type) {
            if (!seen.add(type)) {
                return;
            }
            if (type instanceof JavaTypeFactoryImpl.SyntheticRecordType) {
                memberDeclarations.add(
                    classDecl((JavaTypeFactoryImpl.SyntheticRecordType) type));
            }
            if (type instanceof ParameterizedType) {
                for (Type type1 : ((ParameterizedType) type).getActualTypeArguments()) {
                    register(type1);
                }
            }
        }

        public void go(BlockStatement blockStatement) {
            final Set<Type> types = new LinkedHashSet<>();
            blockStatement.accept(new EnumerableRelImplementor.TypeFinder(types));
            //types.add(result.physType.getJavaRowType());
            for (Type type : types) {
                register(type);
            }
        }
    }
}
