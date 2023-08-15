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

package io.dingodb.expr.runtime.eval;

import io.dingodb.expr.runtime.EvalContext;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddDouble;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddFloat;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddInt;
import io.dingodb.expr.runtime.eval.arithmetic.add.AddLong;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivDouble;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivFloat;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivInt;
import io.dingodb.expr.runtime.eval.arithmetic.div.DivLong;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulDouble;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulFloat;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulInt;
import io.dingodb.expr.runtime.eval.arithmetic.mul.MulLong;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegDouble;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegFloat;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegInt;
import io.dingodb.expr.runtime.eval.arithmetic.neg.NegLong;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosDouble;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosFloat;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosInt;
import io.dingodb.expr.runtime.eval.arithmetic.pos.PosLong;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubDouble;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubFloat;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubInt;
import io.dingodb.expr.runtime.eval.arithmetic.sub.SubLong;
import io.dingodb.expr.runtime.eval.cast.toBool.DoubleToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.FloatToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.IntToBool;
import io.dingodb.expr.runtime.eval.cast.toBool.LongToBool;
import io.dingodb.expr.runtime.eval.cast.toDouble.FloatToDouble;
import io.dingodb.expr.runtime.eval.cast.toDouble.IntToDouble;
import io.dingodb.expr.runtime.eval.cast.toDouble.LongToDouble;
import io.dingodb.expr.runtime.eval.cast.toFloat.DoubleToFloat;
import io.dingodb.expr.runtime.eval.cast.toFloat.IntToFloat;
import io.dingodb.expr.runtime.eval.cast.toFloat.LongToFloat;
import io.dingodb.expr.runtime.eval.cast.toInt.DoubleToInt;
import io.dingodb.expr.runtime.eval.cast.toInt.FloatToInt;
import io.dingodb.expr.runtime.eval.cast.toInt.LongToInt;
import io.dingodb.expr.runtime.eval.cast.toLong.DoubleToLong;
import io.dingodb.expr.runtime.eval.cast.toLong.FloatToLong;
import io.dingodb.expr.runtime.eval.cast.toLong.IntToLong;
import io.dingodb.expr.runtime.eval.logical.AndEval;
import io.dingodb.expr.runtime.eval.logical.NotEval;
import io.dingodb.expr.runtime.eval.logical.OrEval;
import io.dingodb.expr.runtime.eval.logical.VarArgAndEval;
import io.dingodb.expr.runtime.eval.logical.VarArgOrEval;
import io.dingodb.expr.runtime.eval.relational.eq.EqBool;
import io.dingodb.expr.runtime.eval.relational.eq.EqDouble;
import io.dingodb.expr.runtime.eval.relational.eq.EqFloat;
import io.dingodb.expr.runtime.eval.relational.eq.EqInt;
import io.dingodb.expr.runtime.eval.relational.eq.EqLong;
import io.dingodb.expr.runtime.eval.relational.eq.EqString;
import io.dingodb.expr.runtime.eval.relational.ge.GeBool;
import io.dingodb.expr.runtime.eval.relational.ge.GeDouble;
import io.dingodb.expr.runtime.eval.relational.ge.GeFloat;
import io.dingodb.expr.runtime.eval.relational.ge.GeInt;
import io.dingodb.expr.runtime.eval.relational.ge.GeLong;
import io.dingodb.expr.runtime.eval.relational.ge.GeString;
import io.dingodb.expr.runtime.eval.relational.gt.GtBool;
import io.dingodb.expr.runtime.eval.relational.gt.GtDouble;
import io.dingodb.expr.runtime.eval.relational.gt.GtFloat;
import io.dingodb.expr.runtime.eval.relational.gt.GtInt;
import io.dingodb.expr.runtime.eval.relational.gt.GtLong;
import io.dingodb.expr.runtime.eval.relational.gt.GtString;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseBool;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseDouble;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseFloat;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseInt;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseLong;
import io.dingodb.expr.runtime.eval.relational.isFalse.IsFalseString;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullBool;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullDouble;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullFloat;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullInt;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullLong;
import io.dingodb.expr.runtime.eval.relational.isNull.IsNullString;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueBool;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueDouble;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueFloat;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueInt;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueLong;
import io.dingodb.expr.runtime.eval.relational.isTrue.IsTrueString;
import io.dingodb.expr.runtime.eval.relational.le.LeBool;
import io.dingodb.expr.runtime.eval.relational.le.LeDouble;
import io.dingodb.expr.runtime.eval.relational.le.LeFloat;
import io.dingodb.expr.runtime.eval.relational.le.LeInt;
import io.dingodb.expr.runtime.eval.relational.le.LeLong;
import io.dingodb.expr.runtime.eval.relational.le.LeString;
import io.dingodb.expr.runtime.eval.relational.lt.LtBool;
import io.dingodb.expr.runtime.eval.relational.lt.LtDouble;
import io.dingodb.expr.runtime.eval.relational.lt.LtFloat;
import io.dingodb.expr.runtime.eval.relational.lt.LtInt;
import io.dingodb.expr.runtime.eval.relational.lt.LtLong;
import io.dingodb.expr.runtime.eval.relational.lt.LtString;
import io.dingodb.expr.runtime.eval.relational.ne.NeBool;
import io.dingodb.expr.runtime.eval.relational.ne.NeDouble;
import io.dingodb.expr.runtime.eval.relational.ne.NeFloat;
import io.dingodb.expr.runtime.eval.relational.ne.NeInt;
import io.dingodb.expr.runtime.eval.relational.ne.NeLong;
import io.dingodb.expr.runtime.eval.relational.ne.NeString;
import io.dingodb.expr.runtime.eval.value.BoolValue;
import io.dingodb.expr.runtime.eval.value.DoubleValue;
import io.dingodb.expr.runtime.eval.value.FloatValue;
import io.dingodb.expr.runtime.eval.value.IntValue;
import io.dingodb.expr.runtime.eval.value.LongValue;
import io.dingodb.expr.runtime.eval.value.StringValue;
import io.dingodb.expr.runtime.eval.var.IndexedVar;
import io.dingodb.expr.runtime.eval.var.NamedVar;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Evaluator implements EvalVisitor<Object> {
    public static final Evaluator SIMPLE = new Evaluator(null);

    private final EvalContext context;

    public static Evaluator of(EvalContext context) {
        if (context != null) {
            return new Evaluator(context);
        }
        return SIMPLE;
    }

    @Override
    public Object visit(@NonNull IntValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull LongValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull FloatValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull DoubleValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull BoolValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull StringValue eval) {
        return eval.getValue();
    }

    @Override
    public Object visit(@NonNull LongToInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull FloatToInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DoubleToInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IntToLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull FloatToLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DoubleToLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IntToFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LongToFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DoubleToFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IntToDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LongToDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull FloatToDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IntToBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LongToBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull FloatToBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DoubleToBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull PosInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull PosLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull PosFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull PosDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NegInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NegLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NegFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NegDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull AddInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull AddLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull AddFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull AddDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull SubInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull SubLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull SubFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull SubDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull MulInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull MulLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull MulFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull MulDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DivInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DivLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DivFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull DivDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull EqString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GeString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull GtString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LeString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull LtString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NeString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsNullString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsTrueString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseInt eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseLong eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseFloat eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseDouble eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseBool eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IsFalseString eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NotEval eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull AndEval eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull OrEval eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull VarArgAndEval eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull VarArgOrEval eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull IndexedVar eval) {
        return null;
    }

    @Override
    public Object visit(@NonNull NamedVar eval) {
        return null;
    }
}
