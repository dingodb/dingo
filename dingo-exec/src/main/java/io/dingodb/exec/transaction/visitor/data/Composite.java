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

package io.dingodb.exec.transaction.visitor.data;

import io.dingodb.exec.transaction.visitor.Visitor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.List;


@Getter
@Setter
@SuperBuilder
public class Composite implements Element {

    public String name;
    protected List<Element> children;

    public void add(Element element) {
        children.add(element);
    }

    public void remove(Element element) {
        children.remove(element);
    }

    @Override
    public Element getData() {
        return null;
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        for (Element child : children) {
            child.accept(visitor);
        }
        visitor.visit(this);
        return null;
    }
}
