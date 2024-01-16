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

package io.dingodb.calcite.rule;

import io.dingodb.calcite.DingoTable;
import io.dingodb.calcite.rel.DingoLikeScan;
import io.dingodb.calcite.rel.DingoTableScan;
import io.dingodb.calcite.type.converter.DefinitionMapper;
import io.dingodb.calcite.utils.RexLiteralUtils;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.meta.entity.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

@Slf4j
@Value.Enclosing
public class DingoLikeRule extends RelRule<DingoLikeRule.Config> {

    public DingoLikeRule(Config config) {
        super(config);
    }

    static RexLiteral getPrefix(RexLiteral rexLiteral) {
        StringBuilder stringBuilder = new StringBuilder();
        // Process redundant ''
        String patternStr = rexLiteral.toString().replaceAll("'", "");
        if (patternStr.trim().length() == 0) {
            return null;
        }

        char[] prefixChars = patternStr.toCharArray();
        // Record the previous one to determine whether the wildcard is escaped
        char previousChar = 0;
        for (char prefixChar : prefixChars) {
            // If it is % or _ or [, indicates a wildcard is encountered
            if (prefixChar == '%' || prefixChar == '_' || prefixChar == '[') {
                // If the previous bit is not '\\', the prefix has ended
                if (previousChar != '\\') {
                    break;
                }
            }
            previousChar = prefixChar;
            stringBuilder.append(prefixChar);
        }

        if (stringBuilder.length() == 0) {
            return null;
        }

        return RexLiteral.fromJdbcString(rexLiteral.getType(), rexLiteral.getTypeName(), stringBuilder.toString());
    }

    @Override
    public void onMatch(@NonNull RelOptRuleCall call) {
        final DingoTableScan rel = call.rel(0);
        Table td = rel.getTable().unwrap(DingoTable.class).getTable();
        int firstPrimaryColumnIndex = td.keyMapping().get(0);
        RexCall filter = (RexCall) rel.getFilter();

        RexLiteral rexLiteral = (RexLiteral) filter.operands.get(1);
        RexLiteral prefix = getPrefix(rexLiteral);
        if (prefix == null) {
            log.warn("The prefix is empty, original filter string is {}", rexLiteral);
            return;
        }

        RexNode rexNode = filter.operands.get(0);
        if (rexNode instanceof RexInputRef) {
            RexInputRef rexInputRef = (RexInputRef) rexNode;
            int index = rexInputRef.getIndex();
            if (index != firstPrimaryColumnIndex) {
                log.warn("The current field is not the primary key of the first column, "
                    + "first primary column is {}, current column is {}", firstPrimaryColumnIndex, index);
                return;
            }
        } else {
            RexCall castNode = (RexCall) rexNode;
            RexInputRef rexInputRef = (RexInputRef) castNode.operands.get(0);
            log.warn("The current column [{}] type is not string", rexInputRef.getIndex());
            return;
        }

        KeyValueCodec codec = CodecService.getDefault().createKeyValueCodec(td.tupleType(), td.keyMapping());
        Object[] tuple = new Object[td.getColumns().size()];

        byte[] prefixBytes;
        tuple[firstPrimaryColumnIndex] = RexLiteralUtils.convertFromRexLiteral(
            prefix, DefinitionMapper.mapToDingoType(prefix.getType())
        );
        prefixBytes = codec.encodeKeyPrefix(tuple, 1);

        if (prefix.getTypeName() == SqlTypeName.CHAR) {
            byte lastByte = prefixBytes[prefixBytes.length - 1];
            if (lastByte < 0) {
                prefixBytes = ByteArrayUtils.slice(prefixBytes, 0, prefixBytes.length - Math.abs(lastByte));
            }
        }

        call.transformTo(
            new DingoLikeScan(
                rel.getCluster(),
                rel.getTraitSet(),
                rel.getHints(),
                rel.getTable(),
                rel.getFilter(),
                rel.getSelection(),
                prefixBytes
            )
        );
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableDingoLikeRule.Config.builder()
            .operandSupplier(
                b0 -> b0.operand(DingoTableScan.class)
                    .predicate(r -> {
                            if (r.getFilter() != null && r.getFilter() instanceof RexCall) {
                                RexCall filter = (RexCall) r.getFilter();
                                return filter.getKind() == SqlKind.LIKE || filter.op.getName().equals("LIKE_BINARY");
                            }
                            return false;
                        }
                    ).noInputs()
            )
            .description("DingoLikeRule")
            .build();

        @Override
        default DingoLikeRule toRule() {
            return new DingoLikeRule(this);
        }
    }
}
