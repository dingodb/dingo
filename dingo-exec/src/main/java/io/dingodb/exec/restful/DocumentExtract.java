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

package io.dingodb.exec.restful;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dingodb.common.restful.RestfulClient;
import io.dingodb.common.restful.RestfulRequest;
import io.dingodb.exec.fun.document.DocumentTextFun;

import java.util.HashMap;
import java.util.Map;

public class DocumentExtract {
    private static final RestfulClient client = new RestfulClient();
    private static final Map<String, RestfulRequest> tokenInterface = new HashMap<>();

    private static final String DOCUMENT_TXT_URL = "http://host:port/text2token";

    private static final String DOCUMENT_RESTFUL_METHOD = "PUT";

    static {
        // restful url is temp constant
        // use to demonstrate
        tokenInterface.put(DocumentTextFun.NAME,
            new RestfulRequest(DOCUMENT_TXT_URL, null, DOCUMENT_RESTFUL_METHOD, ""));
    }

    public static String[] getTxtDocument(String funName, String host, Object param) {
        host = removeQuote(host);
        RestfulRequest request = tokenInterface.get(funName.toLowerCase());
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("text_str", param.toString());
        return getRestFulString(host, request, paramMap);
    }

    private static String removeQuote(String param) {
        if (param == null) {
            throw new RuntimeException("document load param error");
        }
        if (param.contains("'")) {
            param = param.replace("'", "");
        }
        return param;
    }

    private static String[] getRestFulString(String host, RestfulRequest request, Map<String, Object> paramMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            request.setParam(objectMapper.writeValueAsString(paramMap));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        request.setHost(host);
        String response = client.put(request.getUrl(), request.getParam());
        if (response == null) {
            throw new RuntimeException("document load error");
        }
        ObjectMapper objectMapperRes = new ObjectMapper();
        try {
            return objectMapperRes.readValue(response, String[].class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
