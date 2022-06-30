package io.dingodb.sdk.client;

import io.dingodb.sdk.common.Filter;
import io.dingodb.sdk.common.Operation;
import io.dingodb.sdk.common.Processor;
import io.dingodb.sdk.utils.DingoClientException;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.function.Function;

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
     * @param objects One or two objects to save.
     * @throws DingoClientException an Runtime Exception will be thrown in case of an error.
     */
    void save(@NotNull Object... objects);

    /**
     * Save an object in the database. This method will perform a REPLACE on the existing record so any existing
     * data will be overwritten by the data in the passed object
     *
     * @param object The object to save.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    void save(@NotNull Object object, String... columnNames);


    /**
     * Updates the object in the database, merging the record with the existing record.
     * If Columns are specified, only columns with the passed names will be updated (or all of them if null is passed)
     * @param object The object to update.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    void update(@NotNull Object object, String... columnNames);

    /**
     * Read a record from the repository and map it to an instance of the passed class.
     *
     * @param clazz   - The type of be returned.
     * @param userKey - The key of the record. The database and table will be derived from the values specified on the passed class.
     * @return The returned mapped record.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> T read(@NotNull Class<T> clazz, @NotNull Object userKey);

    /**
     * This method should not be used: It is used by mappers to correctly resolved dependencies.
     */
    <T> T read(@NotNull Class<T> clazz, @NotNull Object userKey, boolean resolveDependencies);

    /**
     * Read a batch of records from database and map them to an instance of the passed class.
     *
     * @param clazz    - The type of be returned.
     * @param userKeys - The keys of the record. The database and table will be derived from the values specified on the passed class.
     * @return The returned mapped records.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[] userKeys);

    /**
     * Read a batch of records from the repository using read operations in one batch call and map them to an instance of the passed class.
     *
     * @param clazz      - The type of be returned.
     * @param userKeys   - The keys of the record. The database and table will be derived from the values specified on the passed class.
     * @param operations - array of read operations on record.
     * @return The returned mapped records.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> T[] read(@NotNull Class<T> clazz, @NotNull Object[] userKeys, Operation... operations);

    /**
     * Delete a record by specifying a class and a user key.
     *
     * @param clazz   - The type of the record.
     * @param userKey - The key of the record. The database and table will be derived from the values specified on the passed class.
     * @return whether record existed on server before deletion
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    <T> boolean delete(@NotNull Class<T> clazz, @NotNull Object userKey);

    /**
     * Delete a record by specifying an object.
     *
     * @param object The object to delete.
     * @return whether record existed on server before deletion
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     */
    boolean delete(@NotNull Object object);

    /**
     * Find a record by specifying a class and a Boolean function.
     *
     * @param clazz    - The type of the record.
     * @param function a Boolean function.
     * @throws DingoClientException an DingoClientException will be thrown in case of an error.
     * @deprecated use the scan/query APIs instead.
     */
    @Deprecated
    <T> void find(@NotNull Class<T> clazz, Function<T, Boolean> function);

    /**
     * Scan every record in the set associated with the passed class. Each record will be converted to the appropriate class then passed to the
     * processor. If the processor returns true, more records will be processed and if the processor returns false, the scan is aborted.
     * <p/>
     * Depending on the ScanPolicy set up for this class, it is possible for the processor to be called by multiple different
     * threads concurrently, so the processor should be thread-safe
     *
     * @param clazz     - the class used to determine which set to scan and to convert the returned records to.
     * @param processor - the Processor used to process each record
     */
    <T> void scan(@NotNull Class<T> clazz, @NotNull Processor<T> processor);


    /**
     * Scan every record in the set associated with the passed class, limiting the throughput to the specified recordsPerSecond. Each record will be converted
     * to the appropriate class then passed to the
     * processor. If the processor returns true, more records will be processed and if the processor returns false, the scan is aborted.
     * <p/>
     * Depending on the ScanPolicy set up for this class, it is possible for the processor to be called by multiple different
     * threads concurrently, so the processor should be thread-safe
     *
     * @param clazz            - the class used to determine which set to scan and to convert the returned records to.
     * @param processor        - the Processor used to process each record
     * @param recordsPerSecond - the maximum number of records to be processed every second.
     */
    <T> void scan(@NotNull Class<T> clazz, @NotNull Processor<T> processor, int recordsPerSecond);

    /**
     * Scan every record in the set associated with the passed class
     * and returns the list of records converted to the appropriate class.
     *
     * @param clazz - the class used to determine which set to scan and to convert the returned records to.
     */
    <T> List<T> scan(@NotNull Class<T> clazz);


    /**
     * Perform a secondary index query with the specified query policy. Each record will be converted
     * to the appropriate class then passed to the processor. If the processor returns false the query is aborted
     * whereas if the processor returns true subsequent records (if any) are processed.
     * <p/>
     * The query policy used will be the one associated with the passed classtype.
     *
     * @param clazz     - the class used to determine which set to scan and to convert the returned records to.
     * @param processor - the Processor used to process each record
     * @param filter    - the filter used to determine which secondary index to use. If this filter is null, every record in the set
     *                  associated with the passed classtype will be scanned, effectively turning the query into a scan
     */
    <T> void query(@NotNull Class<T> clazz, @NotNull Processor<T> processor, Filter filter);

    /**
     * Perform a secondary index query with the specified query policy
     * and returns the list of records converted to the appropriate class.
     * <p/>
     * The query policy used will be the one associated with the passed classtype.
     *
     * @param clazz  - the class used to determine which set to scan and to convert the returned records to.
     * @param filter - the filter used to determine which secondary index to use. If this filter is null, every record in the set
     *               associated with the passed classtype will be scanned, effectively turning the query into a scan
     * @return List of records converted to the appropriate class
     */
    <T> List<T> query(@NotNull Class<T> clazz, Filter filter);


    DingoClient getClient();
}
