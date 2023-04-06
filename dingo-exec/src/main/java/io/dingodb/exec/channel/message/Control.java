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

package io.dingodb.exec.channel.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.dingodb.expr.json.runtime.Parser;
import io.dingodb.net.Message;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(StopTx.class),
    @JsonSubTypes.Type(IncreaseBuffer.class),
})
public abstract class Control {
    private static final Parser PARSER = Parser.JSON;

    @Getter
    @JsonProperty("tag")
    private final String tag;

    protected Control(
        String tag
    ) {
        this.tag = tag;
    }

    public static Control fromMessage(@NonNull Message message) throws JsonProcessingException {
        String str = new String(message.content(), StandardCharsets.UTF_8);
        return PARSER.parse(str, Control.class);
    }

    public byte[] toBytes() throws JsonProcessingException {
        return PARSER.stringify(this).getBytes(StandardCharsets.UTF_8);
    }
}
