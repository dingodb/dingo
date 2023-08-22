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
import io.dingodb.expr.runtime.eval.arithmetic.AddDouble;
import io.dingodb.expr.runtime.eval.arithmetic.AddFloat;
import io.dingodb.expr.runtime.eval.arithmetic.AddInt;
import io.dingodb.expr.runtime.eval.arithmetic.AddLong;
import io.dingodb.expr.runtime.eval.arithmetic.DivDouble;
import io.dingodb.expr.runtime.eval.arithmetic.DivFloat;
import io.dingodb.expr.runtime.eval.arithmetic.DivInt;
import io.dingodb.expr.runtime.eval.arithmetic.DivLong;
import io.dingodb.expr.runtime.eval.arithmetic.MulDouble;
import io.dingodb.expr.runtime.eval.arithmetic.MulFloat;
import io.dingodb.expr.runtime.eval.arithmetic.MulInt;
import io.dingodb.expr.runtime.eval.arithmetic.MulLong;
import io.dingodb.expr.runtime.eval.arithmetic.NegDouble;
import io.dingodb.expr.runtime.eval.arithmetic.NegFloat;
import io.dingodb.expr.runtime.eval.arithmetic.NegInt;
import io.dingodb.expr.runtime.eval.arithmetic.NegLong;
import io.dingodb.expr.runtime.eval.arithmetic.PosDouble;
import io.dingodb.expr.runtime.eval.arithmetic.PosFloat;
import io.dingodb.expr.runtime.eval.arithmetic.PosInt;
import io.dingodb.expr.runtime.eval.arithmetic.PosLong;
import io.dingodb.expr.runtime.eval.arithmetic.SubDouble;
import io.dingodb.expr.runtime.eval.arithmetic.SubFloat;
import io.dingodb.expr.runtime.eval.arithmetic.SubInt;
import io.dingodb.expr.runtime.eval.arithmetic.SubLong;
import io.dingodb.expr.runtime.eval.cast.DoubleToBool;
import io.dingodb.expr.runtime.eval.cast.DoubleToFloat;
import io.dingodb.expr.runtime.eval.cast.DoubleToInt;
import io.dingodb.expr.runtime.eval.cast.DoubleToLong;
import io.dingodb.expr.runtime.eval.cast.FloatToBool;
import io.dingodb.expr.runtime.eval.cast.FloatToDouble;
import io.dingodb.expr.runtime.eval.cast.FloatToInt;
import io.dingodb.expr.runtime.eval.cast.FloatToLong;
import io.dingodb.expr.runtime.eval.cast.IntToBool;
import io.dingodb.expr.runtime.eval.cast.IntToDouble;
import io.dingodb.expr.runtime.eval.cast.IntToFloat;
import io.dingodb.expr.runtime.eval.cast.IntToLong;
import io.dingodb.expr.runtime.eval.cast.LongToBool;
import io.dingodb.expr.runtime.eval.cast.LongToDouble;
import io.dingodb.expr.runtime.eval.cast.LongToFloat;
import io.dingodb.expr.runtime.eval.cast.LongToInt;
import io.dingodb.expr.runtime.eval.logical.AndEval;
import io.dingodb.expr.runtime.eval.logical.NotEval;
import io.dingodb.expr.runtime.eval.logical.OrEval;
import io.dingodb.expr.runtime.eval.logical.VarArgAndEval;
import io.dingodb.expr.runtime.eval.logical.VarArgOrEval;
import io.dingodb.expr.runtime.eval.relational.EqBool;
import io.dingodb.expr.runtime.eval.relational.EqDouble;
import io.dingodb.expr.runtime.eval.relational.EqFloat;
import io.dingodb.expr.runtime.eval.relational.EqInt;
import io.dingodb.expr.runtime.eval.relational.EqLong;
import io.dingodb.expr.runtime.eval.relational.EqString;
import io.dingodb.expr.runtime.eval.relational.GeBool;
import io.dingodb.expr.runtime.eval.relational.GeDouble;
import io.dingodb.expr.runtime.eval.relational.GeFloat;
import io.dingodb.expr.runtime.eval.relational.GeInt;
import io.dingodb.expr.runtime.eval.relational.GeLong;
import io.dingodb.expr.runtime.eval.relational.GeString;
import io.dingodb.expr.runtime.eval.relational.GtBool;
import io.dingodb.expr.runtime.eval.relational.GtDouble;
import io.dingodb.expr.runtime.eval.relational.GtFloat;
import io.dingodb.expr.runtime.eval.relational.GtInt;
import io.dingodb.expr.runtime.eval.relational.GtLong;
import io.dingodb.expr.runtime.eval.relational.GtString;
import io.dingodb.expr.runtime.eval.relational.IsFalseBool;
import io.dingodb.expr.runtime.eval.relational.IsFalseDouble;
import io.dingodb.expr.runtime.eval.relational.IsFalseFloat;
import io.dingodb.expr.runtime.eval.relational.IsFalseInt;
import io.dingodb.expr.runtime.eval.relational.IsFalseLong;
import io.dingodb.expr.runtime.eval.relational.IsFalseString;
import io.dingodb.expr.runtime.eval.relational.IsNullBool;
import io.dingodb.expr.runtime.eval.relational.IsNullDouble;
import io.dingodb.expr.runtime.eval.relational.IsNullFloat;
import io.dingodb.expr.runtime.eval.relational.IsNullInt;
import io.dingodb.expr.runtime.eval.relational.IsNullLong;
import io.dingodb.expr.runtime.eval.relational.IsNullString;
import io.dingodb.expr.runtime.eval.relational.IsTrueBool;
import io.dingodb.expr.runtime.eval.relational.IsTrueDouble;
import io.dingodb.expr.runtime.eval.relational.IsTrueFloat;
import io.dingodb.expr.runtime.eval.relational.IsTrueInt;
import io.dingodb.expr.runtime.eval.relational.IsTrueLong;
import io.dingodb.expr.runtime.eval.relational.IsTrueString;
import io.dingodb.expr.runtime.eval.relational.LeBool;
import io.dingodb.expr.runtime.eval.relational.LeDouble;
import io.dingodb.expr.runtime.eval.relational.LeFloat;
import io.dingodb.expr.runtime.eval.relational.LeInt;
import io.dingodb.expr.runtime.eval.relational.LeLong;
import io.dingodb.expr.runtime.eval.relational.LeString;
import io.dingodb.expr.runtime.eval.relational.LtBool;
import io.dingodb.expr.runtime.eval.relational.LtDouble;
import io.dingodb.expr.runtime.eval.relational.LtFloat;
import io.dingodb.expr.runtime.eval.relational.LtInt;
import io.dingodb.expr.runtime.eval.relational.LtLong;
import io.dingodb.expr.runtime.eval.relational.LtString;
import io.dingodb.expr.runtime.eval.relational.NeBool;
import io.dingodb.expr.runtime.eval.relational.NeDouble;
import io.dingodb.expr.runtime.eval.relational.NeFloat;
import io.dingodb.expr.runtime.eval.relational.NeInt;
import io.dingodb.expr.runtime.eval.relational.NeLong;
import io.dingodb.expr.runtime.eval.relational.NeString;
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
