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

import com.google.auto.service.AutoService;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.dingodb.expr.core.TypeCode;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class EvaluatorsProcessor extends AbstractProcessor {
    private static final String EVALUATOR_EVAL_METHOD = "eval";
    private static final String EVALUATOR_TYPE_CODE_METHOD = "typeCode";
    private static final String EVALUATORS_VAR = "evaluators";

    private static Stream<TypeName> getParaTypeStream(@NonNull ExecutableElement element) {
        return element.getParameters().stream()
            .map(VariableElement::asType)
            .map(TypeName::get);
    }

    /**
     * Calculate a weight for a method by its parameter types, for sorting.
     *
     * @param element   the method element
     * @param typeOrder the defined types order
     * @return an integer stand for the weight
     */
    private static int methodWeight(ExecutableElement element, @NonNull List<TypeName> typeOrder) {
        return getParaTypeStream(element)
            .mapToInt(p -> findTypeIndex(typeOrder, p))
            .sum();
    }

    private static @NonNull String getEvaluatorKey(@Nullable List<TypeName> paraTypes) {
        StringBuilder b = new StringBuilder();
        if (paraTypes != null) {
            for (TypeName type : paraTypes) {
                b.append(StringUtils.capitalize(TypeCode.nameOf(CoreUtils.typeCode(type)).toLowerCase()));
            }
            return b.toString();
        }
        return "Universal";
    }

    private static @NonNull FieldSpec serialVersionUid() {
        return FieldSpec.builder(TypeName.LONG, "serialVersionUID")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$LL", new Random().nextLong())
            .build();
    }

    private static @Nullable ExecutableElement getMethodByNameAndParaTypes(
        @NonNull TypeElement element,
        String name,
        List<TypeName> paraTypes
    ) {
        List<ExecutableElement> methods = ElementFilter.methodsIn(element.getEnclosedElements());
        for (ExecutableElement m : methods) {
            if (m.getSimpleName().toString().equals(name)) {
                if (paraTypes == null) {
                    return m;
                }
                List<TypeName> types = m.getParameters().stream()
                    .map(Element::asType)
                    .map(TypeName::get)
                    .collect(Collectors.toList());
                if (types.equals(paraTypes)) {
                    return m;
                }
            }
        }
        return null;
    }

    private static @NonNull CodeBlock codeEvalParas(
        @NonNull EvaluatorsInfo info,
        String methodName,
        String evalMethodParaName,
        @NonNull List<TypeName> paras,
        List<TypeName> newParas
    ) {
        CodeBlock.Builder codeBuilder = CodeBlock.builder();
        codeBuilder.add("return $T.$L(", info.getOriginClass(), methodName);
        List<CodeBlock> paraBlocks = new ArrayList<>(paras.size());
        for (int i = 0; i < paras.size(); ++i) {
            paraBlocks.add(CoreUtils.codeCasting(
                CodeBlock.of("$L[$L]", evalMethodParaName, i),
                paras.get(i),
                newParas.get(i),
                info.isCheckRange()
            ));
        }
        codeBuilder.add(CodeBlock.join(paraBlocks, ", "));
        codeBuilder.add(");\n");
        return codeBuilder.build();
    }

    private static int findTypeIndex(@NonNull List<TypeName> typeList, TypeName type) {
        int index = typeList.indexOf(type);
        if (index < 0) {
            if (type.isPrimitive()) {
                index = typeList.indexOf(type.box());
            } else if (type.isBoxedPrimitive()) {
                index = typeList.indexOf(type.unbox());
            }
        }
        return index;
    }

    // Helper to get annotation value of type `Class<?>`
    private @Nullable AnnotationValue getAnnotationValue(
        @NonNull AnnotationMirror annotationMirror,
        String methodName
    ) {
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry
            : processingEnv.getElementUtils().getElementValuesWithDefaults(annotationMirror).entrySet()) {
            if (entry.getKey().getSimpleName().toString().equals(methodName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private List<TypeElement> findSuperTypes(@NonNull TypeElement element) {
        return processingEnv.getTypeUtils().directSupertypes(element.asType()).stream()
            .filter(i -> i.getKind() == TypeKind.DECLARED)
            .map(TypeMirror::toString)
            .map(processingEnv.getElementUtils()::getTypeElement)
            .collect(Collectors.toList());
    }

    private @Nullable ExecutableElement getOverridingMethod(
        @NonNull TypeElement element,
        String name,
        List<TypeName> paraTypes
    ) {
        ExecutableElement method = getMethodByNameAndParaTypes(element, name, paraTypes);
        if (method != null) {
            if (!method.getModifiers().contains(Modifier.FINAL)) {
                return method;
            }
            return null;
        }
        for (TypeElement e : findSuperTypes(element)) {
            method = getOverridingMethod(e, name, paraTypes);
            if (method != null) {
                return method;
            }
        }
        return null;
    }

    private @Nullable AnnotationMirror getAnnotationMirror(
        Element element,
        @SuppressWarnings("SameParameterValue") Class<?> annotationClass
    ) {
        for (AnnotationMirror am : processingEnv.getElementUtils().getAllAnnotationMirrors(element)) {
            if (am.getAnnotationType().toString().equals(
                annotationClass.getName().replace('$', '.')
            )) {
                return am;
            }
        }
        return null;
    }

    private @Nullable TypeElement getTypeElementFromAnnotationValue(
        @NonNull AnnotationMirror annotationMirror,
        String methodName
    ) {
        AnnotationValue value = getAnnotationValue(annotationMirror, methodName);
        if (value != null) {
            // com.sun.tools.javac.code.Type.ClassType
            TypeMirror type = (TypeMirror) value.getValue();
            TypeElement element = processingEnv.getElementUtils().getTypeElement(type.toString());
            if (element == null) {
                throw new IllegalStateException("Cannot find a class of name \"" + type + "\".");
            }
            return element;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private @Nullable List<TypeName> getTypeNamesFromAnnotationValue(
        @NonNull AnnotationMirror annotationMirror,
        @SuppressWarnings("SameParameterValue") String methodName
    ) {
        AnnotationValue value = getAnnotationValue(annotationMirror, methodName);
        if (value == null) {
            return null;
        }
        List<AnnotationValue> annotationValues = (List<AnnotationValue>) value.getValue();
        List<TypeName> typeNames = new ArrayList<>(annotationValues.size());
        // com.sun.tools.javac.util.List<com.sun.tools.javac.code.Attribute.Class>
        for (AnnotationValue attr : (List<AnnotationValue>) value.getValue()) {
            TypeMirror t = (TypeMirror) attr.getValue();
            typeNames.add(TypeName.get(t));
        }
        return typeNames;
    }

    private void generateEvaluator(
        final @NonNull ExecutableElement element,
        final @NonNull List<TypeName> paras,
        final @NonNull List<TypeName> newParas,
        final @NonNull EvaluatorsInfo info
    ) {
        String methodName = element.getSimpleName().toString();
        Pattern pattern = Pattern.compile("^([a-zA-Z]+)\\d*$");
        Matcher matcher = pattern.matcher(methodName);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Not a valid method name: \"" + methodName + "\".");
        }
        String evaluatorName = StringUtils.capitalize(matcher.group(1));
        Map<String, EvaluatorInfo> evaluatorMap = info.getEvaluatorMap();
        String evaluatorKey = getEvaluatorKey(newParas);
        if (evaluatorMap.containsKey(evaluatorKey)) {
            return;
        }
        TypeElement evaluatorBase = info.getEvaluatorBase();
        ExecutableElement evalMethod = getOverridingMethod(
            evaluatorBase,
            EVALUATOR_EVAL_METHOD,
            Collections.singletonList(TypeName.get(Object[].class))
        );
        if (evalMethod == null) {
            return;
        }
        String paraName = evalMethod.getParameters().get(0).getSimpleName().toString();
        TypeName returnType = TypeName.get(element.getReturnType()).box();
        MethodSpec evalSpec = MethodSpec.overriding(evalMethod)
            .returns(returnType)
            .addCode(codeEvalParas(info, methodName, paraName, paras, newParas))
            .build();
        ExecutableElement typeCodeMethod = getOverridingMethod(
            evaluatorBase,
            EVALUATOR_TYPE_CODE_METHOD,
            null
        );
        MethodSpec typeCodeSpec = null;
        if (typeCodeMethod != null) {
            typeCodeSpec = MethodSpec.overriding(typeCodeMethod)
                .addStatement("return $L", CoreUtils.typeOf(returnType))
                .build();
        }
        String className = evaluatorName + evaluatorKey;
        TypeSpec.Builder builder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .addField(serialVersionUid())
            .addMethod(evalSpec);
        if (evaluatorBase.getKind().isInterface()) {
            builder.addSuperinterface(evaluatorBase.asType());
        } else {
            builder.superclass(evaluatorBase.asType());
        }
        if (typeCodeSpec != null) {
            builder.addMethod(typeCodeSpec);
        }
        String packageName = info.getPackageName();
        // must copy newParas, it is volatile.
        evaluatorMap.put(
            evaluatorKey,
            new EvaluatorInfo(className, returnType, new ArrayList<>(newParas), builder.build())
        );
    }

    private void induceEvaluators(ExecutableElement element, EvaluatorsInfo info) throws IOException {
        List<TypeName> paras = getParaTypeStream(element).collect(Collectors.toList());
        // must make a copy of paras
        List<TypeName> newParas = paras.stream()
            .map(TypeName::box)
            .collect(Collectors.toList());
        induceEvaluatorsRecursive(element, paras, newParas, info, 0);
    }

    private void tryDescentType(
        final @NonNull ExecutableElement element,
        final @NonNull List<TypeName> paras,
        @NonNull List<TypeName> newParas,
        final @NonNull EvaluatorsInfo info,
        int pos
    ) throws IOException {
        List<TypeName> induceSequence = info.getInduceSequence();
        TypeName type = paras.get(pos);
        int index = findTypeIndex(induceSequence, type);
        if (index < 0) {
            return;
        }
        for (int i = index + 1; i < induceSequence.size(); ++i) {
            TypeName newTypeName = induceSequence.get(i);
            TypeName oldType = newParas.get(pos);
            newParas.set(pos, newTypeName);
            induceEvaluatorsRecursive(element, paras, newParas, info, pos + 1);
            newParas.set(pos, oldType);
        }
    }

    private void induceEvaluatorsRecursive(
        final @NonNull ExecutableElement element,
        final @NonNull List<TypeName> paras,
        @NonNull List<TypeName> newParas,
        final @NonNull EvaluatorsInfo info,
        int pos
    ) throws IOException {
        if (pos >= newParas.size()) {
            generateEvaluator(element, paras, newParas, info);
            return;
        }
        induceEvaluatorsRecursive(element, paras, newParas, info, pos + 1);
        tryDescentType(element, paras, newParas, info, pos);
    }

    private void generateEvaluatorFactories(@NonNull EvaluatorsInfo info) throws IOException {
        String packageName = info.getPackageName();
        TypeElement evaluatorKey = info.getEvaluatorKey();
        TypeElement evaluatorFactory = info.getEvaluatorFactory();
        TypeElement universalEvaluator = info.getUniversalEvaluator();
        Map<String, EvaluatorInfo> evaluatorMap = info.getEvaluatorMap();
        CodeBlock.Builder initBuilder = CodeBlock.builder();
        TypeName generalReturnType = null;
        for (Map.Entry<String, EvaluatorInfo> entry : evaluatorMap.entrySet()) {
            EvaluatorInfo evaluatorInfo = entry.getValue();
            TypeName returnType = evaluatorInfo.getReturnTypeName();
            if (generalReturnType == null) {
                generalReturnType = returnType;
            } else if (generalReturnType != TypeName.OBJECT) {
                if (!returnType.equals(generalReturnType)) {
                    generalReturnType = TypeName.OBJECT;
                }
            }
            List<TypeName> paraTypeNames = evaluatorInfo.getParaTypeNames();
            initBuilder.addStatement("$L.put($L, new $N())",
                EVALUATORS_VAR,
                CoreUtils.evaluatorKeyOf(evaluatorKey, paraTypeNames),
                evaluatorInfo.getInnerClass()
            );
        }
        initBuilder.addStatement("$L.put($T.UNIVERSAL, new $T(this, $L))",
            EVALUATORS_VAR,
            evaluatorKey,
            ClassName.get(universalEvaluator),
            CoreUtils.typeOf(generalReturnType)
        );
        ClassName className = ClassName.get(
            packageName,
            info.getOriginClass().getSimpleName().toString() + "Factory"
        );
        TypeSpec.Builder builder = TypeSpec.classBuilder(className)
            .superclass(TypeName.get(evaluatorFactory.asType()))
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addField(serialVersionUid())
            .addField(FieldSpec.builder(className, ProcessorUtils.INSTANCE_VAR_NAME)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("new $T()", className)
                .build())
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addStatement("super()")
                .addCode(initBuilder.build())
                .build());
        for (Map.Entry<String, EvaluatorInfo> entry : evaluatorMap.entrySet()) {
            builder.addType(entry.getValue().getInnerClass());
        }
        TypeSpec typeSpec = builder.build();
        ProcessorUtils.saveSourceFile(processingEnv, packageName, typeSpec);
    }

    private void generateEvaluators(@NonNull TypeElement element, EvaluatorsInfo info) throws IOException {
        Element pkg = element.getEnclosingElement();
        if (pkg.getKind() != ElementKind.PACKAGE) {
            throw new IllegalStateException("Class annotated with \"Evaluators\" must not be an inner class.");
        }
        info.setPackageName(pkg.asType().toString());
        info.setOriginClass(element);
        info.setEvaluatorMap(new HashMap<>());
        List<ExecutableElement> executableElements = ElementFilter.methodsIn(element.getEnclosedElements());
        executableElements.sort(Comparator.comparingInt(e -> -methodWeight(e, info.getInduceSequence())));
        for (ExecutableElement executableElement : executableElements) {
            induceEvaluators(executableElement, info);
        }
        generateEvaluatorFactories(info);
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Evaluators.class.getName());
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations == null) {
            return true;
        }
        for (TypeElement annotation : annotations) {
            if (annotation.getQualifiedName().contentEquals(Evaluators.class.getCanonicalName())) {
                try {
                    Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(annotation);
                    for (Element element : elements) {
                        if (!(element instanceof TypeElement)) {
                            throw new IllegalStateException("Element annotated with \"Evaluators\" must be a class.");
                        }
                        AnnotationMirror annotationMirror = getAnnotationMirror(element, Evaluators.class);
                        if (annotationMirror == null) {
                            throw new IllegalStateException("This should never show up.");
                        }
                        TypeElement evaluatorKey = getTypeElementFromAnnotationValue(
                            annotationMirror,
                            "evaluatorKey"
                        );
                        TypeElement evaluatorBase = getTypeElementFromAnnotationValue(
                            annotationMirror,
                            "evaluatorBase"
                        );
                        TypeElement evaluatorFactory = getTypeElementFromAnnotationValue(
                            annotationMirror,
                            "evaluatorFactory"
                        );
                        TypeElement universalEvaluator = getTypeElementFromAnnotationValue(
                            annotationMirror,
                            "universalEvaluator"
                        );
                        List<TypeName> induceSequence = getTypeNamesFromAnnotationValue(
                            annotationMirror,
                            "induceSequence"
                        );
                        boolean rangeCheck = element.getAnnotation(Evaluators.class).checkRange();
                        EvaluatorsInfo info = new EvaluatorsInfo(
                            evaluatorKey,
                            evaluatorBase,
                            evaluatorFactory,
                            universalEvaluator,
                            induceSequence,
                            rangeCheck
                        );
                        generateEvaluators((TypeElement) element, info);
                    }
                } catch (IOException e) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getLocalizedMessage());
                }
            }
        }
        return true;
    }
}
