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

package io.dingodb.client.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Columns marked with DingoEmbed will have the objects
 * they reference embedded in the parent object, either as a list or a map.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DingoEmbed {

    enum EmbedType {
        LIST,
        MAP,
        DEFAULT
    }

    EmbedType type() default EmbedType.DEFAULT;

    /**
     * The elementType is used for sub-elements. For example, if there is:
     * <pre>
     * &#064;DingoEmbed (elementType = EmbedType.LIST)
     * private List&lt;Account&gt; accounts;
     * </pre>
     * then the objects will be stored in the database as lists of lists, rather than lists of maps.
     * @return embed type
     */
    EmbedType elementType() default EmbedType.DEFAULT;

    /**
     * Determine whether the key should be saved in the sub-object. This is used only for translating
     * a list of objects in Java into a map of objects which contains a list. For example:
     * <pre>
     * public class Transaction {
     *    private String name;
     *    private int value;
     *    &#064;DingoKey
     *    private long time;
     *
     *    public Transaction() {}
     *    public Transaction(String name, int value, long time) {
     *        super();
     *        this.name = name;
     *        this.value = value;
     *        this.time = time;
     *    }
     * }
     *
     * ...
     *
     * &#064;DingoEmbed(type = EmbedType.MAP, elementType = EmbedType.LIST)
     * public List&lt;Transaction&gt; txns;
     * </pre>
     * Assume elements have been inserted into the list as follows:
     * <pre>
     *  account.txns.add(new Transaction("details1", 100, 101));
     *  account.txns.add(new Transaction("details2", 200, 99));
     *  account.txns.add(new Transaction("details3", 300, 1010));
     * </pre>
     * Since these elements are stored in a map, the DingoKey field (time in this case) will be the key to the map.
     * By default, the key is then dropped from the list as it is not needed, resulting in:
     * <pre>
     * KEY_ORDERED_MAP('{99:["details2", 200], 101:["details1", 100], 1010:["details3", 300]}')
     * </pre>
     *
     * <p>If it is desired for the key to be part of the list as well, this value can be set to <code>true</code>:
     *
     * <pre>
     * &#064;DingoEmbed(type = EmbedType.MAP, elementType = EmbedType.LIST, saveKey = true)
     * public List&lt;Transaction&gt; txns;
     * </pre>
     * This would result in:
     * <pre>
     * KEY_ORDERED_MAP('{99:["details2", 99, 200], 101:["details1", 101, 100], 1010:["details3", 1010, 300]}')
     * </pre>
     * @return default false
     */
    boolean saveKey() default false;
}
