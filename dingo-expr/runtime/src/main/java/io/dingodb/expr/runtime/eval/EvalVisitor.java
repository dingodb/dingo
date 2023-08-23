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
