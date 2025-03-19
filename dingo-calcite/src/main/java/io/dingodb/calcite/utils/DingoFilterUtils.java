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

import io.dingodb.common.util.Pair;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;

public class DingoFilterUtils {

    public static boolean enableIndexRangeFilter(RexNode rexFilter) {
        if (rexFilter instanceof RexCall) {
            RexCall rexCall = (RexCall) rexFilter;
            boolean andMatch;
            if (rexCall.getKind() == SqlKind.AND) {
                if (rexCall.getOperands().size() > 1) {
                    andMatch = rexCall.getOperands().stream().allMatch(sub -> sub instanceof RexCall);
                    return andMatch;
                }
            } else {
                return true;
            }
        }
        return false;
    }

    public static Pair<RexNode, RexNode> splitRexFilter(RexNode rexFilter, Mapping mapping, RexBuilder rexBuilder) {
        if (rexFilter instanceof RexCall) {
            RexCall rexCall = (RexCall) rexFilter;
            List<RexNode> indexCallList = new ArrayList<>();
            List<RexNode> otherCallList = new ArrayList<>();
            if (rexCall.getKind() == SqlKind.AND) {
                List<RexNode> list = rexCall.getOperands();
                for (RexNode subCall : list) {
                    try {
                        RexNode subRexFilter = RexUtil.apply(mapping, subCall);
                        indexCallList.add(subCall);
                    } catch (Exception e) {
                        otherCallList.add(subCall);
                    }
                }
            }
            if (indexCallList.isEmpty()) {
                return null;
            }
            RexNode indexCall = rexBuilder.makeCall(rexCall.op, indexCallList);
            RexNode otherCall = null;
            if (!otherCallList.isEmpty()) {
                otherCall = rexBuilder.makeCall(rexCall.op, otherCallList);
            }
            return Pair.of(indexCall, otherCall);
        }
        return null;
    }

}
