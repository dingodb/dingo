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

package io.dingodb.proxy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.dingodb.expr.runtime.op.OpType;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.proxy.handler.ExprOpDeserializer;
import io.dingodb.proxy.handler.ExprTypeDeserializer;
import io.dingodb.proxy.handler.ScalarValueDeserializer;
import io.dingodb.proxy.handler.StringDataSerializer;
import io.dingodb.proxy.handler.VectorIndexParameterDeserializer;
import io.dingodb.sdk.service.entity.common.ScalarField.DataNest.StringData;
import io.dingodb.sdk.service.entity.common.ScalarValue;
import io.dingodb.sdk.service.entity.common.VectorIndexParameter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    public static ObjectMapper jsonMapper;

    @Bean
    public VectorIndexParameterDeserializer addVectorIndexParameterDeserializer(@Autowired ObjectMapper mapper) {
        jsonMapper = mapper;
        SimpleModule simpleModule = new SimpleModule();
        VectorIndexParameterDeserializer vectorIndexParameterDeserializer = new VectorIndexParameterDeserializer();
        simpleModule.addDeserializer(VectorIndexParameter.class, vectorIndexParameterDeserializer);
        mapper.registerModule(simpleModule);
        return vectorIndexParameterDeserializer;
    }

    @Bean
    public ScalarValueDeserializer addScalarValueDeserializer(@Autowired ObjectMapper mapper) {
        jsonMapper = mapper;
        SimpleModule simpleModule = new SimpleModule();
        ScalarValueDeserializer deserializer = new ScalarValueDeserializer();
        simpleModule.addDeserializer(ScalarValue.class, deserializer);
        mapper.registerModule(simpleModule);
        return deserializer;
    }


    @Bean
    public StringDataSerializer addStringDataSerializer(@Autowired ObjectMapper mapper) {
        jsonMapper = mapper;
        SimpleModule simpleModule = new SimpleModule();
        StringDataSerializer serializer = new StringDataSerializer();
        simpleModule.addSerializer(StringData.class, serializer);
        mapper.registerModule(simpleModule);
        return serializer;
    }

    @Bean
    public ExprOpDeserializer addExprOpDeserializer(@Autowired ObjectMapper mapper) {
        SimpleModule simpleModule = new SimpleModule();
        ExprOpDeserializer deserializer = new ExprOpDeserializer();
        simpleModule.addDeserializer(OpType.class, deserializer);
        mapper.registerModule(simpleModule);
        return deserializer;
    }

    @Bean
    public ExprTypeDeserializer addExprTypeDeserializer(@Autowired ObjectMapper mapper) {
        SimpleModule simpleModule = new SimpleModule();
        ExprTypeDeserializer deserializer = new ExprTypeDeserializer();
        simpleModule.addDeserializer(Type.class, deserializer);
        mapper.registerModule(simpleModule);
        return deserializer;
    }
}
