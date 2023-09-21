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

package io.dingodb.client;

import io.dingodb.client.common.Filter;
import io.dingodb.client.common.Key;
import io.dingodb.client.common.Processor;
import io.dingodb.sdk.common.DingoClientException;

import java.util.List;
import javax.validation.constraints.NotNull;

public interface DingoMapper extends IBaseDingoMapper {

    /**
     * Save each object in the database. This method will perform a REPLACE on the existing record so any existing
     * data will be overwritten by the data in the passed object. This is a convenience method for
     * <pre>
     * save(A);
     * save(B);
     * save(C);
     * </pre>
     * Not that no transactionality is implied by this method -- if any of the save methods fail, the exception will be
     * thrown without trying the other objects, nor attempting to roll back previously saved objects
     *
     * @param objectArray One or multiple objects to save.
     * @throws DingoClientException a runtime Exception will be thrown in case of an error.
     */
    void save(@NotNull Object[] objectArray);

    /**
     * Save an object in the database. This method will perform a REPLACE on the existing record so any existing
     * data will be overwritten by the data in the passed object
     *
     * @param object The object to save.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    void save(@NotNull Object object);


    /**
     * Updates the object in the database, merging the record with the existing record.
     * If Columns are specified, only columns with the passed names will be updated (or all of them if null is passed)
     *
     * @param object The object to update.
     * @param columnNames column names
     * @return is success
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    boolean update(@NotNull Object object, String... columnNames);

    /**
     * Read a record from the repository and map it to an instance of the passed class.
     *
     * @param <T> class
     * @param clazz   - The type of be returned.
     * @param userKey - The key of the record.
     *                The database and table will be derived from the values specified on the passed class.
     * @return The returned mapped record.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> T read(@NotNull Class<T> clazz, @NotNull Object[] userKey);


    /**
     * Read a batch of records from database and map them to an instance of the passed class.
     *
     * @param <T> class
     * @param clazz    - The type of be returned.
     * @param userKeys - The keys of the record.
     *                 The database and table will be derived from the values specified on the passed class.
     * @return The returned mapped records.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[][] userKeys);

    /**
     * Delete a record by a user Key.
     *
     * @param tableName table name.
     * @param userKey - The key of the record.
     * @return whether record existed on server before deletion
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    boolean delete(String tableName, Key userKey);

    /**
     * Delete a record by specifying an object.
     *
     * @param record The record to delete.
     * @return whether record existed on server before deletion
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    boolean delete(@NotNull Object record);

    /**
     * Perform a secondary index query with the specified query policy. Each record will be converted
     * to the appropriate class then passed to the processor. If the processor returns false the query is aborted
     * whereas if the processor returns true subsequent records (if any) are processed.
     * The query policy used will be the one associated with the passed classtype.
     *
     * @param <T> class
     * @param clazz     - the class used to determine which set to scan and to convert the returned records to.
     * @param processor - the Processor used to process each record
     * @param filter    - the filter used to determine which secondary index to use.
     *                  If this filter is null, every record in the set
     *                  associated with the passed classtype will be scanned, effectively turning the query into a scan
     */
    <T> void query(@NotNull Class<T> clazz, @NotNull Processor<T> processor, Filter filter);

    /**
     * Perform a secondary index query with the specified query policy
     * and returns the list of records converted to the appropriate class.
     * The query policy used will be the one associated with the passed classtype.
     *
     * @param <T> class
     * @param clazz  - the class used to determine which set to scan and to convert the returned records to.
     * @param filter - the filter used to determine which secondary index to use.
     *               If this filter is null, every record in the set
     *               associated with the passed classtype will be scanned, effectively turning the query into a scan
     * @return List of records converted to the appropriate class
     */
    <T> List<T> query(@NotNull Class<T> clazz, Filter filter);

}
