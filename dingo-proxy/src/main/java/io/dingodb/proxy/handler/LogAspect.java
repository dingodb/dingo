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

package io.dingodb.proxy.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.sdk.common.DingoClientException;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Aspect
public class LogAspect {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogAspect.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Pointcut("execution(public * io.dingodb.web.controller.*.*(..))")
    public void log() {

    }

    @Around("log()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        String methodName = point.getSignature().toLongString();
        Object[] parameterValues = point.getArgs();
        String[] parameterNames = ((MethodSignature) point.getSignature()).getParameterNames();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("methodName: {}, parameters: {}", methodName, mapper.writeValueAsString(assembleParameter(parameterNames, parameterValues)));
        }

        Object result;
        try {
            result = point.proceed();
        } catch (Exception e) {
            LOGGER.error("methodName: {}, exception", methodName, e);
            throw e;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("methodName: {}, response: {}", methodName, mapper.writeValueAsString(result));
        }
        return result;

    }

    private Map<String, Object> assembleParameter(String[] parameterNames, Object[] parameterValues) {
        Map<String, Object> parameterNameAndValues = new HashMap<>();
        for (int i = 0; i < parameterNames.length; i++) {
            parameterNameAndValues.put(parameterNames[i], parameterValues[i]);
        }
        return parameterNameAndValues;
    }
}
