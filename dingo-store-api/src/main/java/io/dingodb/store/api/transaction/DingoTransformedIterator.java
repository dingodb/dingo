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

package io.dingodb.store.api.transaction;

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Function;
import io.dingodb.common.profile.Profile;
import lombok.Getter;

import java.util.Iterator;

@GwtCompatible
public class DingoTransformedIterator<F, T> implements Iterator<T> {
    public static <F, T> Iterator<T> transform(Iterator<F> fromIterator, final Function<? super F, ? extends T> function) {
        return new DingoTransformedIterator<F, T>(fromIterator, function);
    }

    @Getter
    private Profile profile;

    Iterator<? extends F> backingIterator;

    final Function<? super F, ? extends T> function;

    public DingoTransformedIterator(Iterator<F> fromIterator, Function<? super F, ? extends T> function) {
        this.backingIterator = fromIterator;
        if (fromIterator instanceof ProfileScanIterator) {
            profile = ((ProfileScanIterator) fromIterator).getRpcProfile();
        }
        this.function = function;
    }

    T transform(F var1) {
        return function.apply(var1);
    }

    @Override
    public boolean hasNext() {
        return this.backingIterator.hasNext();
    }

    @Override
    public T next() {
        return this.transform(this.backingIterator.next());
    }

    public final void remove() {
        this.backingIterator.remove();
    }
}
