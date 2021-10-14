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

package io.dingodb.expr.runtime.evaluator.base;

import io.dingodb.expr.runtime.TypeCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

@ToString
public final class EvaluatorKey implements Serializable {
    public static final EvaluatorKey UNIVERSAL = new EvaluatorKey(null);
    private static final long serialVersionUID = 3094073337324796122L;

    @Getter
    private final int[] paraTypeCodes;

    private EvaluatorKey(int[] paraTypeCodes) {
        this.paraTypeCodes = paraTypeCodes;
    }

    /**
     * Create a EvaluatorKey of specified type codes.
     *
     * @param paraTypeCodes the type codes
     * @return the EvaluatorKey
     */
    @Nonnull
    public static EvaluatorKey of(int... paraTypeCodes) {
        return new EvaluatorKey(paraTypeCodes);
    }

    @Nonnull
    private EvaluatorKey copy() {
        return new EvaluatorKey(paraTypeCodes.clone());
    }

    /**
     * Return a list of EvaluatorKey with each parameter generalized to OBJECT.
     *
     * @return the list
     */
    @Nonnull
    public List<EvaluatorKey> generalize() {
        List<EvaluatorKey> keys = new ArrayList<>(9);
        int i = 0;
        EvaluatorKey key = copy();
        while (true) {
            if (i == paraTypeCodes.length) {
                keys.add(key.copy());
                i--;
                if (key.paraTypeCodes[i] == TypeCode.OBJECT) {
                    while (i >= 0 && key.paraTypeCodes[i] == TypeCode.OBJECT) {
                        key.paraTypeCodes[i] = this.paraTypeCodes[i];
                        i--;
                    }
                    if (i < 0) {
                        break;
                    }
                }
                key.paraTypeCodes[i] = TypeCode.OBJECT;
            }
            i++;
        }
        keys.add(EvaluatorKey.UNIVERSAL);
        return keys;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(paraTypeCodes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EvaluatorKey) {
            return Arrays.equals(paraTypeCodes, ((EvaluatorKey) obj).paraTypeCodes);
        }
        return false;
    }
}
