/*
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
package io.trino.plugin.hudi;

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_META_CLIENT_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
import static java.lang.Math.toIntExact;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

public final class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);

    /**
     * Sophos patch (CSA-21894): cached reflective handles for disabling Avro name validation.
     * Writer schemas produced upstream from Scala/Java code paths that use inner-class binary
     * names (e.g. {@code Types$GeoSummary}, {@code File$FileInfo}) embed {@code $} in Avro
     * namespace parts. Avro's default {@code NameValidator} rejects {@code $} per the spec
     * rule {@code [A-Za-z_][A-Za-z0-9_]*}, which fails query planning even though the schemas
     * round-trip through writer/reader fine for Parquet-backed Hudi tables.
     * <p>
     * The disable-validation API differs across Avro versions: 1.11.x exposes the instance
     * setter {@code Parser#setValidate(boolean)} (removed in 1.12); 1.12.x exposes the
     * constructor {@code Parser(NameValidator)} with sentinel {@code NameValidator.NO_VALIDATION}
     * (does not exist in 1.11). We resolve whichever API is available at class-load and reuse
     * the handles per parse, so the same source compiles and runs against either Avro line.
     * <p>
     * Sibling helpers live in {@link HudiTableHandle#buildTableSchema(String)} (worker-side
     * deserialization path) and in the fork's {@code hudi-common} {@code TableSchemaResolver}
     * (coordinator commit-metadata fetch path when {@code hudi-common} is rebuilt from the
     * fork). This copy in {@link HudiUtil} is the one that actually wins in deployment today,
     * because {@code data-presto-lib}'s {@code hudi-trino-plugin} build pulls
     * {@code org.apache.hudi:hudi-common:1.0.2} from Maven Central rather than the fork
     * sources — so a patch in {@code TableSchemaResolver} alone is invisible at runtime.
     */
    private static final Method AVRO_1_11_SET_VALIDATE;
    private static final Constructor<Schema.Parser> AVRO_1_12_PARSER_CTOR;
    private static final Object AVRO_1_12_NO_VALIDATION;

    static {
        Method setValidate = null;
        Constructor<Schema.Parser> ctor = null;
        Object noValidation = null;
        try {
            setValidate = Schema.Parser.class.getMethod("setValidate", boolean.class);
        }
        catch (NoSuchMethodException nsme) {
            try {
                Class<?> nv = Class.forName("org.apache.avro.NameValidator");
                noValidation = nv.getField("NO_VALIDATION").get(null);
                @SuppressWarnings("unchecked")
                Constructor<Schema.Parser> c = (Constructor<Schema.Parser>) Schema.Parser.class.getConstructor(nv);
                ctor = c;
            }
            catch (ReflectiveOperationException roe) {
                log.warn(roe, "Avro on the classpath exposes neither Parser.setValidate(boolean) (1.11.x) "
                        + "nor Parser(NameValidator) (1.12.x); writer schemas containing '$' in namespace "
                        + "parts will fail to parse.");
            }
        }
        AVRO_1_11_SET_VALIDATE = setValidate;
        AVRO_1_12_PARSER_CTOR = ctor;
        AVRO_1_12_NO_VALIDATION = noValidation;
    }

    /**
     * Sophos patch (CSA-21894): parse an Avro schema with name validation disabled.
     * See {@link #AVRO_1_11_SET_VALIDATE} for rationale and the cross-version resolution.
     */
    private static Schema parseSchemaWithoutNameValidation(String schemaStr)
    {
        Schema.Parser parser;
        if (AVRO_1_12_PARSER_CTOR != null) {
            try {
                parser = AVRO_1_12_PARSER_CTOR.newInstance(AVRO_1_12_NO_VALIDATION);
            }
            catch (ReflectiveOperationException roe) {
                parser = new Schema.Parser();
            }
        }
        else {
            parser = new Schema.Parser();
            if (AVRO_1_11_SET_VALIDATE != null) {
                try {
                    AVRO_1_11_SET_VALIDATE.invoke(parser, false);
                }
                catch (ReflectiveOperationException roe) {
                    // parser keeps its default (validating) behavior
                }
            }
        }
        return parser.parse(schemaStr);
    }

    private static final Cache<Schema, Map<String, Schema.Field>> SCHEMA_FIELD_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumWeight(10L * 1000L * 1024L) // 10MB
                    .weigher((Weigher<Schema, Map<String, Schema.Field>>) (schema, fieldMap) -> {
                        // approximate size estimation of schema size
                        long schemaSize = estimatedSizeOf(schema.toString());

                        long fieldsSize = fieldMap.entrySet().stream()
                                .mapToLong(e -> estimatedSizeOf(e.getKey()) + estimatedSizeOf(e.getValue().name()))
                                .sum();

                        return toIntExact(schemaSize + fieldsSize);
                    })
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .shareNothingWhenDisabled()
                    .build();

    private HudiUtil() {}

    public static HoodieFileFormat getHudiFileFormat(String path)
    {
        String extension = getFileExtension(path);
        if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
            return HoodieFileFormat.PARQUET;
        }
        if (extension.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
            return HoodieFileFormat.HOODIE_LOG;
        }
        if (extension.equals(HoodieFileFormat.ORC.getFileExtension())) {
            return HoodieFileFormat.ORC;
        }
        if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
            return HoodieFileFormat.HFILE;
        }
        throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, "Hoodie InputFormat not implemented for base file of type " + extension);
    }

    private static String getFileExtension(String fullName)
    {
        String fileName = Location.of(fullName).fileName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public static boolean hudiMetadataExists(TrinoFileSystem trinoFileSystem, Location baseLocation)
    {
        try {
            Location metaLocation = baseLocation.appendPath(HoodieTableMetaClient.METAFOLDER_NAME);
            FileIterator iterator = trinoFileSystem.listFiles(metaLocation);
            // If there is at least one file in the .hoodie directory, it's a valid Hudi table
            return iterator.hasNext();
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, "Failed to check for Hudi table at location: " + baseLocation, e);
        }
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> partitionValues,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        HivePartition partition = parsePartition(
                tableName, hivePartitionName, partitionColumnHandles, partitionValues);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    /**
     * Copied from {@link io.trino.plugin.hive.HivePartitionManager#parsePartition}
     * to keep partition parsing logic self-contained within {@code trino-hudi}.
     */
    private static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<String> partitionValues)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builderWithExpectedSize(partitionColumns.size());
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), column.getType());
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.buildOrThrow();
        return new HivePartition(tableName, partitionName, values);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HiveColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<HiveColumnHandle> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static HoodieTableMetaClient buildTableMetaClient(
            TrinoFileSystem fileSystem,
            String tableName,
            String basePath)
    {
        try {
            return HoodieTableMetaClient.builder()
                    .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                    .setBasePath(basePath)
                    .build();
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(HUDI_BAD_DATA,
                    "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, basePath));
        }
        catch (Throwable e) {
            throw new TrinoException(HUDI_META_CLIENT_ERROR,
                    "Unable to load Hudi meta client for table %s (%s)".formatted(tableName, basePath));
        }
    }

    public static Schema constructSchema(List<String> columnNames, List<HiveType> columnTypes)
    {
        // Convert lists into the format expected by the utility class
        String columnNamesString = String.join(",", columnNames);
        String columnTypesString = columnTypes.stream()
                .map(HiveType::getHiveTypeName)
                .map(Object::toString)
                .collect(Collectors.joining(":"));

        // Create the properties map
        Map<String, String> properties = new HashMap<>();
        properties.put(LIST_COLUMNS, columnNamesString);
        properties.put(LIST_COLUMN_TYPES, columnTypesString);

        // Call the public static method to build the schema
        try {
            // Pass null for the file system as we are not reading from a URL
            return AvroHiveFileUtils.determineSchemaOrThrowException(null, properties);
        }
        catch (IOException e) {
            // The IOException is declared on the method, but this path shouldn't throw it
            throw new UncheckedIOException("Failed to construct Avro schema", e);
        }
    }

    public static Schema constructSchema(Schema dataSchema, List<String> columnNames)
    {
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record("baseRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();
        for (String columnName : columnNames) {
            Schema.Field field = getFieldFromSchema(columnName, dataSchema);
            Schema originalFieldSchema = field.schema();

            Schema typeForNewField;

            // Check if the original field schema is already nullable (i.e., a UNION containing NULL)
            if (originalFieldSchema.isNullable()) {
                typeForNewField = originalFieldSchema;
            }
            else {
                typeForNewField = Schema.createUnion(Schema.create(Schema.Type.NULL), originalFieldSchema);
            }

            fieldBuilder = fieldBuilder
                    .name(field.name())
                    .type(typeForNewField)
                    .withDefault(null);
        }
        return fieldBuilder.endRecord();
    }

    private static Map<String, Schema.Field> buildFieldLookup(Schema schema)
    {
        return schema.getFields().stream()
                .collect(Collectors.toMap(
                        f -> f.name().toLowerCase(Locale.ROOT),
                        f -> f));
    }

    /**
     * Retrieves a field from the given Avro schema by column name.
     * <p>
     * The lookup proceeds in two steps:
     * <ul>
     *   <li>First, attempts an exact match on the column name.</li>
     *   <li>If not found, falls back to a case-insensitive match using a cached lookup table</li>
     * </ul>
     * <p>
     *
     * @param columnName Column name to search for.
     * @param schema Avro {@link Schema} in which to search.
     * @return The matching {@link Schema.Field}, if found.
     * @throws TrinoException if no field matches the given column name.
     */
    public static Schema.Field getFieldFromSchema(String columnName, Schema schema)
    {
        Schema.Field field = schema.getField(columnName);
        if (field != null) {
            return field;
        }

        try {
            field = SCHEMA_FIELD_CACHE
                    .get(schema, () -> buildFieldLookup(schema)).get(columnName.toLowerCase(Locale.ROOT));
            if (field != null) {
                return field;
            }
        }
        catch (ExecutionException e) {
            throw new TrinoException(HUDI_SCHEMA_ERROR,
                    "Failed to build field lookup for schema", e);
        }

        throw new TrinoException(HUDI_SCHEMA_ERROR,
                "Failed to get column " + columnName + " from table schema");
    }

    public static List<HiveColumnHandle> prependHudiMetaColumns(List<HiveColumnHandle> dataColumns)
    {
        //For efficient lookup
        Set<String> dataColumnNames = dataColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toSet());

        // If all Hudi meta columns are already present, return the original list
        if (dataColumnNames.containsAll(HOODIE_META_COLUMNS)) {
            return dataColumns;
        }

        // Identify only the meta columns that are missing from dataColumns to avoid duplicates
        List<String> missingMetaColumns = HOODIE_META_COLUMNS.stream()
                .filter(metaColumn -> !dataColumnNames.contains(metaColumn))
                .toList();

        List<HiveColumnHandle> columns = new ArrayList<>();

        // Create and prepend the new HiveColumnHandles for the missing meta columns
        columns.addAll(IntStream.range(0, missingMetaColumns.size())
                .boxed()
                .map(i -> new HiveColumnHandle(
                        missingMetaColumns.get(i),
                        i,
                        HiveType.HIVE_STRING,
                        VarcharType.VARCHAR,
                        Optional.empty(),
                        HiveColumnHandle.ColumnType.REGULAR,
                        Optional.empty()))
                .toList());

        // Add all the original data columns after the new meta columns
        columns.addAll(dataColumns);

        return columns;
    }

    public static FileSlice convertToFileSlice(HudiSplit split, String basePath)
    {
        String dataFilePath = split.getBaseFile().isPresent()
                ? split.getBaseFile().get().getPath()
                : split.getLogFiles().getFirst().getPath();
        String fileId = FSUtils.getFileIdFromFileName(new StoragePath(dataFilePath).getName());
        HoodieBaseFile baseFile = split.getBaseFile().isPresent()
                ? new HoodieBaseFile(dataFilePath, fileId, split.getCommitTime(), null)
                : null;

        return new FileSlice(
                new HoodieFileGroupId(FSUtils.getRelativePartitionPath(new StoragePath(basePath), new StoragePath(dataFilePath)), fileId),
                split.getCommitTime(),
                baseFile,
                split.getLogFiles().stream().map(lf -> new HoodieLogFile(lf.getPath())).toList());
    }

    public static HoodieTableFileSystemView getFileSystemView(
            HoodieTableMetadata tableMetadata,
            HoodieTableMetaClient metaClient)
    {
        return new HoodieTableFileSystemView(
                tableMetadata, metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    }

    public static Schema getLatestTableSchema(HoodieTableMetaClient metaClient, String tableName)
    {
        try {
            HoodieTimer timer = HoodieTimer.start();
            Schema schema = readLatestSchemaPermissive(metaClient)
                    .orElseGet(() -> resolveSchemaViaTableSchemaResolver(metaClient));
            log.info("Fetched table schema for table %s in %s ms", tableName, timer.endTimer());
            return schema;
        }
        catch (Exception e) {
            // failed to read schema
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, e);
        }
    }

    /**
     * Sophos patch (CSA-21894): fetch the latest writer schema from commit metadata using
     * only public timeline APIs and parse it through {@link #parseSchemaWithoutNameValidation}.
     * <p>
     * This bypasses {@code new TableSchemaResolver(metaClient).getTableAvroSchema()} on its
     * happy path. {@code TableSchemaResolver} lives in {@code hudi-common}, which our build
     * pulls from Maven Central at {@code 1.0.2} rather than from the Sophos fork — so the
     * patched parser inside the fork's {@code TableSchemaResolver.getTableSchemaFromLatestCommitMetadata}
     * never actually executes in deployment. By doing the timeline read + permissive parse
     * here, in {@code hudi-trino-plugin} (which IS built from fork sources), the fix lands
     * regardless of which {@code hudi-common} jar ends up on the classpath.
     * <p>
     * Returns {@link Optional#empty()} when no completed commit carries a schema (fresh
     * tables, table-create-only configs, log-only datasets). Callers fall back to the
     * full {@link TableSchemaResolver} chain in those cases — that path doesn't trigger
     * the {@code $}-in-namespace failure because it doesn't parse a commit-metadata schema.
     */
    private static Optional<Schema> readLatestSchemaPermissive(HoodieTableMetaClient metaClient)
    {
        HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
        boolean populateMetaFields = metaClient.getTableConfig().populateMetaFields();
        Iterator<HoodieInstant> reverseInstants = timeline.getReverseOrderedInstants().iterator();
        while (reverseInstants.hasNext()) {
            HoodieInstant instant = reverseInstants.next();
            String schemaStr;
            try {
                HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(instant);
                schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
            }
            catch (IOException ioe) {
                // Treat unreadable commit metadata as "no schema here" and walk back to older
                // instants; matches TableSchemaResolver.getLatestCommitMetadataWithValidSchema's
                // filter-then-continue behavior.
                log.warn(ioe, "Failed to read commit metadata for instant %s on table %s; trying older instants",
                        instant, metaClient.getBasePath());
                continue;
            }
            if (schemaStr == null || schemaStr.isEmpty()) {
                continue;
            }
            Schema schema = parseSchemaWithoutNameValidation(schemaStr);
            if (populateMetaFields) {
                // hasOperationField is rarely true for analytics tables and is only inspectable
                // via TableSchemaResolver (which itself triggers a Parquet footer read). The
                // false default matches non-CDC writes; CDC-style operation tracking would
                // need its own path here.
                schema = HoodieAvroUtils.addMetadataFields(schema, false);
            }
            return Optional.of(schema);
        }
        return Optional.empty();
    }

    /**
     * Sophos patch (CSA-21894): edge-case fallback to {@link TableSchemaResolver}. Called only
     * when commit metadata yields no schema (fresh table, table-create-only config, log-only
     * dataset). In those cases the strict-parse code path inside vanilla {@code hudi-common}
     * is not reached because there is no commit-metadata schema string to feed into it.
     */
    private static Schema resolveSchemaViaTableSchemaResolver(HoodieTableMetaClient metaClient)
    {
        try {
            return new TableSchemaResolver(metaClient).getTableAvroSchema();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
