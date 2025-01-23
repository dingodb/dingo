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

import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.exec.restful.VectorExtract;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.util.NlsString;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class VectorUtils {

    private VectorUtils(){}

    private static final int MULTIPLE = 8;

    public static byte[] parseBinaryStringToBinary(List<Object> operandsList) {
        if (operandsList.get(2) instanceof SqlCharStringLiteral) {
            String binaryString = ((NlsString)((SqlCharStringLiteral) operandsList.get(2)).getValue()).getValue();
            int length = binaryString.length();
            int segmentLength = length / MULTIPLE;
            byte[] byteArray = new byte[segmentLength];
            for (int i = 0; i < segmentLength; i++) {
                String segment = binaryString.substring(i * MULTIPLE, (i + 1) * MULTIPLE);
                byteArray[i] = (byte) Integer.parseInt(segment, 2);
            }
            return byteArray;
        }
        throw new RuntimeException("vector load binary string param error");
    }

    public static byte[] parseBinaryStringToByteArray(List<Object> operandsList) {
        if (operandsList.get(2) instanceof  SqlCharStringLiteral) {
            String binaryString = ((NlsString)((SqlCharStringLiteral) operandsList.get(2)).getValue()).getValue();
            int length = binaryString.length();
            byte[] byteArray = new byte[length];
            for (int i = 0; i < length; i++) {
                char c = binaryString.charAt(i);
                byteArray[i] = (byte) c;
            }
            return byteArray;
        }
        throw new RuntimeException("vector load binary string param error");
    }

    public static Float[] getVectorFloats(List<Object> operandsList) {
        Float[] floatArray = null;
        Object call = operandsList.get(2);
        if (call instanceof RexCall) {
            RexCall rexCall = (RexCall) call;
            floatArray = new Float[rexCall.getOperands().size()];
            int vectorDimension = rexCall.getOperands().size();
            for (int i = 0; i < vectorDimension; i++) {
                RexLiteral literal = (RexLiteral) rexCall.getOperands().get(i);
                floatArray[i] = literal.getValueAs(Float.class);
            }
            return floatArray;
        }
        SqlBasicCall basicCall = (SqlBasicCall) operandsList.get(2);
        if (basicCall.getOperator() instanceof SqlArrayValueConstructor) {
            List<SqlNode> operands = basicCall.getOperandList();
            floatArray = new Float[operands.size()];
            for (int i = 0; i < operands.size(); i++) {
                floatArray[i] = (
                    (Number) Objects.requireNonNull(((SqlNumericLiteral) operands.get(i)).getValue())
                ).floatValue();
            }
        } else {
            List<SqlNode> sqlNodes = basicCall.getOperandList();
            if (sqlNodes.size() < 2) {
                throw new RuntimeException("vector load param error");
            }
            List<Object> paramList = sqlNodes.stream().map(e -> {
                if (e instanceof SqlLiteral) {
                    return ((SqlLiteral)e).getValue();
                } else if (e instanceof SqlIdentifier) {
                    return ((SqlIdentifier)e).getSimple();
                } else {
                    return e.toString();
                }
            }).collect(Collectors.toList());
            if (paramList.get(1) == null || paramList.get(0) == null) {
                throw new RuntimeException("vector load param error");
            }
            String param = paramList.get(1).toString();
            if (param.contains("'")) {
                param = param.replace("'", "");
            }
            String funcName = basicCall.getOperator().getName();
            if (funcName.equalsIgnoreCase(VectorTextFun.NAME)) {
                floatArray = VectorExtract.getTxtVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    param);
            } else if (funcName.equalsIgnoreCase(VectorImageFun.NAME)) {
                if (paramList.size() < 3) {
                    throw new RuntimeException("vector load param error");
                }
                Object localPath = paramList.get(2);
                if (!(localPath instanceof Boolean)) {
                    throw new RuntimeException("vector load param error");
                }
                floatArray = VectorExtract.getImgVector(
                    basicCall.getOperator().getName(),
                    paramList.get(0).toString(),
                    paramList.get(1),
                    (Boolean) paramList.get(2));
            }
        }
        if (floatArray == null) {
            throw new RuntimeException("vector load error");
        }
        return floatArray;
    }
}
