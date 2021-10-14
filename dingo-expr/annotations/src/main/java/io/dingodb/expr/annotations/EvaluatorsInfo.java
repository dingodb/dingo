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

package io.dingodb.expr.annotations;

import com.squareup.javapoet.TypeName;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import javax.lang.model.element.TypeElement;

@Getter
@Setter
@RequiredArgsConstructor
public class EvaluatorsInfo {
    private final TypeElement evaluatorKey;
    private final TypeElement evaluatorBase;
    private final TypeElement evaluatorFactory;
    private final TypeElement universalEvaluator;
    private final List<TypeName> induceSequence;

    private String packageName;
    private TypeName originClassName;
    private Map<String, Map<String, EvaluatorInfo>> evaluatorMap;
}
