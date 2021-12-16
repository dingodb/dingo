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

package io.dingodb.dingokv.util;

import io.dingodb.dingokv.cmd.store.BaseRequest;
import io.dingodb.dingokv.errors.Errors;
import io.dingodb.dingokv.errors.InvalidParameterException;
import io.dingodb.dingokv.metadata.RegionEpoch;

import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class KVParameterRequires {
    public static void requireSameEpoch(final BaseRequest request, final RegionEpoch current) {
        RegionEpoch requestEpoch = request.getRegionEpoch();
        if (current.equals(requestEpoch)) {
            return;
        }
        if (current.getConfVer() != requestEpoch.getConfVer()) {
            throw Errors.INVALID_REGION_MEMBERSHIP.exception();
        }
        if (current.getVersion() != requestEpoch.getVersion()) {
            throw Errors.INVALID_REGION_VERSION.exception();
        }
        throw Errors.INVALID_REGION_EPOCH.exception();
    }

    public static <T> T requireNonNull(final T target, final String message) {
        if (target == null) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    public static <T> List<T> requireNonEmpty(final List<T> target, final String message) {
        requireNonNull(target, message);
        if (target.isEmpty()) {
            throw new InvalidParameterException(message);
        }
        return target;
    }

    public static int requireNonNegative(final int value, final String message) {
        if (value < 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static long requireNonNegative(final long value, final String message) {
        if (value < 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static int requirePositive(final int value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    public static long requirePositive(final long value, final String message) {
        if (value <= 0) {
            throw new InvalidParameterException(message);
        }
        return value;
    }

    private KVParameterRequires() {
    }
}
