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

package io.dingodb.exec.transaction.util;

import java.util.ArrayList;
import java.util.List;

public class BinaryVectorUtils {

    private static final int MULTIPLE = 8;

    private BinaryVectorUtils() {}

    public static void checkBinaryVector(byte[] binaryVector, int dimension) {
        if (dimension == 0 || dimension % MULTIPLE != 0) {
            throw new IllegalArgumentException("Dimension must be a positive multiple of " + MULTIPLE);
        }
        if (binaryVector.length != dimension) {
            throw new IllegalArgumentException("The length of the binary vector " +
                "does not match the specified dimension");
        }

        // ASCII values for '0' and '1'
        final byte ASCII_ZERO = 48; // '0'
        final byte ASCII_ONE = 49;  // '1'

        for (int i = 0; i < binaryVector.length; i++) {
            if (binaryVector[i] != ASCII_ZERO && binaryVector[i] != ASCII_ONE) {
                throw new IllegalArgumentException(
                    String.format("Invalid binary value at index %d: expected '0' or '1', but was '%c'.",
                        i, (char)binaryVector[i])
                );
            }
        }
    }

    public static byte[] getBinaryVector(byte[] binaryVector, int dimension) {
        int segmentLength = (dimension + (MULTIPLE - 1)) / MULTIPLE;
        byte[] byteArray = new byte[segmentLength];
        for (int i = 0; i < dimension; i++) {
            byteArray[i / MULTIPLE] |= ((binaryVector[i] & 0x01) << ((MULTIPLE - 1) - (i % MULTIPLE)));
        }
        return byteArray;
    }

    public static List<byte[]> getBinaryVectorList(byte[] binaryVector, int dimension) {
        List<byte[]> result = new ArrayList<>();
        int segmentLength = (dimension + (MULTIPLE - 1)) / MULTIPLE;
        for (int i = 0; i < segmentLength; i++) {
            byte acc = 0;
            int effectiveLength = Math.min(MULTIPLE, dimension - i * MULTIPLE);
            for (int j = 0; j < effectiveLength; j++) {
                int vectorIndex = i * MULTIPLE + j;
                acc |= ((binaryVector[vectorIndex] & 0x01) << (MULTIPLE - 1 - j));
            }
            result.add(new byte[]{acc});
        }
        return result;
    }

    public static int hammingDistance(byte[] x, byte[] y) {
        if (x == null || y == null || x.length != y.length) {
            throw new IllegalArgumentException("Arrays must be non-null and of the same length.");
        }
        int distance = 0;
        for (int i = 0; i < x.length; i++) {
            byte xor = (byte) (x[i] ^ y[i]);
            while (xor != 0) {
                distance++;
                xor = (byte) (xor & (xor - 1));
            }
        }

        return distance;
    }

}
