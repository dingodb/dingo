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
import org.checkerframework.checker.nullness.qual.NonNull;

public interface EvalVisitor<T> {
    T visit(@NonNull IntValue eval);

    T visit(@NonNull LongValue eval);

    T visit(@NonNull FloatValue eval);

    T visit(@NonNull DoubleValue eval);

    T visit(@NonNull BoolValue eval);

    T visit(@NonNull StringValue eval);

    T visit(@NonNull LongToInt eval);

    T visit(@NonNull FloatToInt eval);

    T visit(@NonNull DoubleToInt eval);

    T visit(@NonNull IntToLong eval);

    T visit(@NonNull FloatToLong eval);

    T visit(@NonNull DoubleToLong eval);

    T visit(@NonNull IntToFloat eval);

    T visit(@NonNull LongToFloat eval);

    T visit(@NonNull DoubleToFloat eval);

    T visit(@NonNull IntToDouble eval);

    T visit(@NonNull LongToDouble eval);

    T visit(@NonNull FloatToDouble eval);

    T visit(@NonNull IntToBool eval);

    T visit(@NonNull LongToBool eval);

    T visit(@NonNull FloatToBool eval);

    T visit(@NonNull DoubleToBool eval);

    T visit(@NonNull PosInt eval);

    T visit(@NonNull PosLong eval);

    T visit(@NonNull PosFloat eval);

    T visit(@NonNull PosDouble eval);

    T visit(@NonNull NegInt eval);

    T visit(@NonNull NegLong eval);

    T visit(@NonNull NegFloat eval);

    T visit(@NonNull NegDouble eval);

    T visit(@NonNull AddInt eval);

    T visit(@NonNull AddLong eval);

    T visit(@NonNull AddFloat eval);

    T visit(@NonNull AddDouble eval);

    T visit(@NonNull SubInt eval);

    T visit(@NonNull SubLong eval);

    T visit(@NonNull SubFloat eval);

    T visit(@NonNull SubDouble eval);

    T visit(@NonNull MulInt eval);

    T visit(@NonNull MulLong eval);

    T visit(@NonNull MulFloat eval);

    T visit(@NonNull MulDouble eval);

    T visit(@NonNull DivInt eval);

    T visit(@NonNull DivLong eval);

    T visit(@NonNull DivFloat eval);

    T visit(@NonNull DivDouble eval);

    T visit(@NonNull EqInt eval);

    T visit(@NonNull EqLong eval);

    T visit(@NonNull EqFloat eval);

    T visit(@NonNull EqDouble eval);

    T visit(@NonNull EqBool eval);

    T visit(@NonNull EqString eval);

    T visit(@NonNull GeInt eval);

    T visit(@NonNull GeLong eval);

    T visit(@NonNull GeFloat eval);

    T visit(@NonNull GeDouble eval);

    T visit(@NonNull GeBool eval);

    T visit(@NonNull GeString eval);

    T visit(@NonNull GtInt eval);

    T visit(@NonNull GtLong eval);

    T visit(@NonNull GtFloat eval);

    T visit(@NonNull GtDouble eval);

    T visit(@NonNull GtBool eval);

    T visit(@NonNull GtString eval);

    T visit(@NonNull LeInt eval);

    T visit(@NonNull LeLong eval);

    T visit(@NonNull LeFloat eval);

    T visit(@NonNull LeDouble eval);

    T visit(@NonNull LeBool eval);

    T visit(@NonNull LeString eval);

    T visit(@NonNull LtInt eval);

    T visit(@NonNull LtLong eval);

    T visit(@NonNull LtFloat eval);

    T visit(@NonNull LtDouble eval);

    T visit(@NonNull LtBool eval);

    T visit(@NonNull LtString eval);

    T visit(@NonNull NeInt eval);

    T visit(@NonNull NeLong eval);

    T visit(@NonNull NeFloat eval);

    T visit(@NonNull NeDouble eval);

    T visit(@NonNull NeBool eval);

    T visit(@NonNull NeString eval);

    T visit(@NonNull IsNullInt eval);

    T visit(@NonNull IsNullLong eval);

    T visit(@NonNull IsNullFloat eval);

    T visit(@NonNull IsNullDouble eval);

    T visit(@NonNull IsNullBool eval);

    T visit(@NonNull IsNullString eval);

    T visit(@NonNull IsTrueInt eval);

    T visit(@NonNull IsTrueLong eval);

    T visit(@NonNull IsTrueFloat eval);

    T visit(@NonNull IsTrueDouble eval);

    T visit(@NonNull IsTrueBool eval);

    T visit(@NonNull IsTrueString eval);

    T visit(@NonNull IsFalseInt eval);

    T visit(@NonNull IsFalseLong eval);

    T visit(@NonNull IsFalseFloat eval);

    T visit(@NonNull IsFalseDouble eval);

    T visit(@NonNull IsFalseBool eval);

    T visit(@NonNull IsFalseString eval);

    T visit(@NonNull NotEval eval);

    T visit(@NonNull AndEval eval);

    T visit(@NonNull OrEval eval);

    T visit(@NonNull VarArgAndEval eval);

    T visit(@NonNull VarArgOrEval eval);

    T visit(@NonNull IndexedVar eval);

    T visit(@NonNull NamedVar eval);
}
