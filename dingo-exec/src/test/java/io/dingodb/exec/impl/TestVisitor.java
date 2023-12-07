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

package io.dingodb.exec.impl;

import io.dingodb.exec.transaction.visitor.data.Composite;
import io.dingodb.exec.transaction.visitor.data.Element;
import io.dingodb.exec.transaction.visitor.data.Leaf;
import io.dingodb.exec.transaction.visitor.data.ScanCacheLeaf;
import io.dingodb.exec.transaction.visitor.Visitor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class TestVisitor {

    @Test
    public void testVisitor1() {
        // Build dependencies
        Composite root = Composite.builder()
            .name("Root")
            .children(new ArrayList<>())
            .build();

        Composite branch1 = Composite.builder()
            .name("Branch1")
            .children(new ArrayList<>())
            .build();

        Composite branch2 = Composite.builder()
            .name("Branch2")
            .children(new ArrayList<>())
            .build();

        Leaf leaf1 = Leaf.builder()
            .name("Leaf1")
            .data(null)
            .build();

        Leaf leaf2 = Leaf.builder()
            .name("Leaf2")
            .data(null)
            .build();

        root.add(branch1);
        root.add(branch2);
        branch1.add(leaf1);
        branch2.add(leaf2);

        // Visitor
        Visitor<String> visitor = new TestJobVisitor<>();
        root.accept(visitor);
    }

    @Test
    public void testVisitor2() {
        // Build dependencies

        ScanCacheLeaf scanCache2Leaf = ScanCacheLeaf.builder()
            .name("PreWrite ScanCache")
            .data(null)
            .build();

        Leaf leaf2 = Leaf.builder()
            .name("PreWrite")
            .data(scanCache2Leaf)
            .build();

        ScanCacheLeaf scanCache1Leaf = ScanCacheLeaf.builder()
            .name("Commit ScanCache")
            .data(leaf2)
            .build();

        Leaf leaf1 = Leaf.builder()
            .name("Commit")
            .data(scanCache1Leaf)
            .build();

        Leaf root = Leaf.builder()
            .name("root")
            .data(leaf1)
            .build();

        // Visitor
        Visitor<Element> visitor = new TestJobVisitor<>();
        root.accept(visitor);
    }

}
