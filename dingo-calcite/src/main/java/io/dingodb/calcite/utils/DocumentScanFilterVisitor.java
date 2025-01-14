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

import io.dingodb.common.type.scalar.BooleanType;
import io.dingodb.common.type.scalar.DoubleType;
import io.dingodb.common.type.scalar.LongType;
import io.dingodb.common.type.scalar.StringType;
import io.dingodb.common.type.scalar.TimestampType;
import io.dingodb.meta.entity.Column;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import static io.dingodb.exec.transaction.util.TimeUtils.to;
import static io.dingodb.exec.transaction.util.TimeUtils.toTimeStamp;

public class DocumentScanFilterVisitor extends RexVisitorImpl<DocumentScanFilterOb> {
    private final RexBuilder rexBuilder;

    private final DocumentScanFilterOb documentScanFilterOb;

    public DocumentScanFilterVisitor(RexBuilder rexBuilder, DocumentScanFilterOb documentScanFilterOb) {
        super(true);
        this.rexBuilder = rexBuilder;
        this.documentScanFilterOb = documentScanFilterOb;
    }

    private static @NonNull IndexRangeMapSet<Integer, RexNode> checkOperands(@NonNull RexNode op0, RexNode op1) {
        if (op0.isA(SqlKind.INPUT_REF) && ConstantTester.isConst(op1)) {
            RexInputRef inputRef = (RexInputRef) op0;
            return IndexRangeMapSet.single(inputRef.getIndex(), op1);
        }
        return IndexRangeMapSet.one();
    }

    @Override
    public DocumentScanFilterOb visitInputRef(@NonNull RexInputRef inputRef) {
        String rightName = inputRef.getType().getSqlTypeName().getName();
        if (rightName.equalsIgnoreCase("BOOLEAN")) {
            int index = inputRef.getIndex();
            Column column = this.documentScanFilterOb.getColumns().get(index);
//            String value = ((NlsString) Objects.requireNonNull(((RexLiteral) inputRef).getValue())).getValue();
            this.documentScanFilterOb.setQueryStr(
                this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":true");
            this.documentScanFilterOb.setMatch(true);
        }
        return this.documentScanFilterOb;
    }

    // `null` means the RexNode is not related to primary column
    @Override
    public DocumentScanFilterOb visitCall(@NonNull RexCall call) {
        List<RexNode> operands = call.getOperands();
        switch (call.getKind()) {
            case CAST: {
                for (RexNode operand : operands) {
                    operand.accept(this);
                }
                return this.documentScanFilterOb;
            }
            case OR: {
                int n = 0;
                for (RexNode operand : operands) {
                    operand.accept(this);
                    n++;
                    if (documentScanFilterOb.isMatch() && n != operands.size()) {
                        documentScanFilterOb.setQueryStr(documentScanFilterOb.getQueryStr() + " OR");
                    }
                }
                return this.documentScanFilterOb;
            }
            case AND: {
                int n = 0;
                for (RexNode operand : operands) {
                    operand.accept(this);
                    n++;
                    if (documentScanFilterOb.isMatch() && n != operands.size()) {
                        documentScanFilterOb.setQueryStr(documentScanFilterOb.getQueryStr() + " AND");
                    }
                }
                return this.documentScanFilterOb;
            }
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                RexNode leftRexNode = operands.get(0);
                RexNode rightRexNode = operands.get(1);

                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                    this.documentScanFilterOb.setMatch(false);
                    return this.documentScanFilterOb;
                }
                String leftName = leftRexNode.getType().getSqlTypeName().getName();
                if(leftRexNode instanceof RexInputRef && (leftName.equalsIgnoreCase("VARCHAR") ||
                    leftName.equalsIgnoreCase("TIMESTAMP")
                    || leftName.equalsIgnoreCase("BOOLEAN"))){
                    this.documentScanFilterOb.setMatch(false);
                    return this.documentScanFilterOb;
                }
                String rightName = rightRexNode.getType().getSqlTypeName().getName();
                if(rightRexNode instanceof RexInputRef  && (leftName.equalsIgnoreCase("VARCHAR") ||
                    rightName.equalsIgnoreCase("TIMESTAMP")
                    || rightName.equalsIgnoreCase("BOOLEAN"))){
                    this.documentScanFilterOb.setMatch(false);
                    return this.documentScanFilterOb;
                }
                if (leftRexNode instanceof RexLiteral && rightRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) rightRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof LongType || column.getType() instanceof DoubleType) {
                        String value = Objects.requireNonNull(((RexLiteral) leftRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase()
                                + ":" + call.getOperator().getName() + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }
                }
                if (rightRexNode instanceof RexLiteral && leftRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) leftRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof LongType || column.getType() instanceof DoubleType) {
                        String value = Objects.requireNonNull(((RexLiteral) rightRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase()
                                + ":" + call.getOperator().getName() + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }
                }
                this.documentScanFilterOb.setMatch(false);
                return this.documentScanFilterOb;
            case EQUALS: {
                leftRexNode = operands.get(0);
                rightRexNode = operands.get(1);

                if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                    this.documentScanFilterOb.setMatch(false);
                }
                if (leftRexNode instanceof RexLiteral && rightRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) rightRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof StringType) {
                        String value = ((NlsString) Objects.requireNonNull(((RexLiteral) leftRexNode).getValue())).getValue();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":\"" + value + "\"");
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    } else if (column.getType() instanceof LongType || column.getType() instanceof DoubleType) {
                        String value = Objects.requireNonNull(((RexLiteral) leftRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":" + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }
                }
                if (rightRexNode instanceof RexLiteral && leftRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) leftRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof StringType) {
                        String value = ((NlsString) Objects.requireNonNull(((RexLiteral) rightRexNode).getValue())).getValue();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":\"" + value + "\"");
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    } else if (column.getType() instanceof LongType || column.getType() instanceof DoubleType) {
                        String value = Objects.requireNonNull(((RexLiteral) rightRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":" + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }  else if (column.getType() instanceof TimestampType) {
                        String value = Objects.requireNonNull(((RexLiteral) rightRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":" + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }  else if (column.getType() instanceof BooleanType) {
                        String value = Objects.requireNonNull(((RexLiteral) rightRexNode).getValue()).toString();
                        this.documentScanFilterOb.setQueryStr(
                            this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase() + ":" + value);
                        this.documentScanFilterOb.setMatch(true);
                        return this.documentScanFilterOb;
                    }
                }
                if (rightRexNode instanceof RexCall && leftRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) leftRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof TimestampType && rightRexNode.getKind() == SqlKind.CAST) {
                        if (((RexCall) rightRexNode).operands.get(0) instanceof RexLiteral) {
                            RexNode rexNode = ((RexCall) rightRexNode).operands.get(0);
                            String value = ((NlsString) Objects.requireNonNull(((RexLiteral) rexNode).getValue())).getValue();
                            Timestamp timeStamp = toTimeStamp(value);
                            if (timeStamp == null) {
                                this.documentScanFilterOb.setMatch(false);
                                return this.documentScanFilterOb;
                            }
                            this.documentScanFilterOb.setQueryStr(
                                this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase()
                                    + ":\"" + to(timeStamp) + "\"");
                            this.documentScanFilterOb.setMatch(true);
                            return this.documentScanFilterOb;
                        }
                    }
                }
                if (leftRexNode instanceof RexCall && rightRexNode instanceof RexInputRef) {
                    int index = ((RexInputRef) rightRexNode).getIndex();
                    Column column = this.documentScanFilterOb.getColumns().get(index);
                    if (column.getType() instanceof TimestampType && leftRexNode.getKind() == SqlKind.CAST) {
                        if (((RexCall) leftRexNode).operands.get(0) instanceof RexLiteral) {
                            RexNode rexNode = ((RexCall) leftRexNode).operands.get(0);
                            String value = ((NlsString) Objects.requireNonNull(((RexLiteral) rexNode).getValue())).getValue();
                            Timestamp timeStamp = toTimeStamp(value);
                            if (timeStamp == null) {
                                this.documentScanFilterOb.setMatch(false);
                                return this.documentScanFilterOb;
                            }
                            this.documentScanFilterOb.setQueryStr(
                                this.documentScanFilterOb.getQueryStr() + " " + column.getName().toUpperCase()
                                    + ":\"" + to(timeStamp) + "\"");
                            this.documentScanFilterOb.setMatch(true);
                            return this.documentScanFilterOb;
                        }
                    }
                }
                this.documentScanFilterOb.setMatch(false);
                return this.documentScanFilterOb;
            }
            default:
                break;
        }
        return this.documentScanFilterOb;
    }
}
