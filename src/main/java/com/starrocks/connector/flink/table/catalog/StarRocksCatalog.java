package com.starrocks.connector.flink.table.catalog;

import com.starrocks.connector.flink.table.StarRocksTypeMapper;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.UnsupportedDataTypeException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StarRocksCatalog extends AbstractCatalog {

    private static final Logger log = LoggerFactory.getLogger(StarRocksCatalog.class);

    private final String username;
    private final String password;
    private final String jdbcUrl;
    private final String defaultUrl;

    private final String databaseVersion;

    private static final Set<String> builtinDatabases = ImmutableSet.of(
            "_statistics_", "information_schema"
    );

    public StarRocksCatalog(String catalogName,
                            String defaultDatabase,
                            String username,
                            String password,
                            String jdbcUrl) {
        super(catalogName, defaultDatabase);

        this.username = username;
        this.password = password;
        this.jdbcUrl = jdbcUrl.endsWith("/") ? jdbcUrl : jdbcUrl + "/";
        this.defaultUrl = this.jdbcUrl + defaultDatabase;

        this.databaseVersion = Preconditions.checkNotNull(
                getDatabaseVersion(), "Database version must not be null");

        log.info("database version : {}", databaseVersion);

    }


    @Override
    public void open() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, password)) {
        } catch (Exception e) {
            throw new ValidationException(String.format("Failed connecting to %s via JDBC", defaultUrl), e);
        }

        log.info("Catalog {} established connection to {}", getName(), defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        log.info("Catalog {} closing", getName());
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName)
        );
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        Map<String, String> properties = extractColumnMapValuesBySQL(
                jdbcUrl,
                "SELECT * FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = ?;",
                databaseName
        ).get(0);
        return new CatalogDatabaseImpl(properties, null);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return !extractColumnValuesBySQL(
                jdbcUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE SCHEMA_NAME = ?;",
                1,
                v -> true,
                databaseName
        ).isEmpty();
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfExists) throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        }
        String sql = "CREATE DATABASE IF NOT EXISTS " + databaseName;
        executeSql(jdbcUrl, sql);
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                databaseName != null && !databaseName.isEmpty(),
                "Database name must not be empty."
        );

        return extractColumnValuesBySQL(
                jdbcUrl + databaseName,
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                v -> true,
                databaseName
        );
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        String dbUrl = jdbcUrl + tablePath.getDatabaseName();
        List<Map<String, String>> columns = extractColumnMapValuesBySQL(
                dbUrl,
                "SELECT `COLUMN_NAME`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;",
                tablePath.getDatabaseName(),
                tablePath.getObjectName()
        );

        StringBuilder pkName = new StringBuilder("pk");
        List<String> pkColumnNames = new LinkedList<>();
        String[] columnNames = new String[columns.size()];
        DataType[] columnTypes = new DataType[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Map<String, String> column = columns.get(i);
            columnNames[i] = column.get("COLUMN_NAME");
            columnTypes[i] = StarRocksTypeMapper.mapping(column.get("DATA_TYPE"), column.get("COLUMN_SIZE"), column.get("DECIMAL_DIGITS"));
            if ("PRI".equals(column.get("COLUMN_KEY"))) {
                pkName.append("_").append(columnNames[i]);
                pkColumnNames.add(columnNames[i]);
            }
        }
        Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, columnTypes);

        if (!pkColumnNames.isEmpty()) {
            schemaBuilder.primaryKeyNamed(pkName.toString(), pkColumnNames);
        }

        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "starrocks");

        List<String> tableMeta = extractColumnValuesBySQL(
                dbUrl,
                "SHOW CREATE TABLE " + tablePath.getObjectName(),
                1,
                null
        );
        options.put("sr-createMeta", tableMeta.get(0));

        return CatalogTable.of(schemaBuilder.build(), null, Collections.emptyList(), options);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return !extractColumnMapValuesBySQL(
                jdbcUrl,
                "SELECT `COLUMN_NAME`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;",
                tablePath.getDatabaseName(),
                tablePath.getObjectName()
        ).isEmpty();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable catalogBaseTable, boolean ignoreIfNotExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableAlreadyExistException(getName(), tablePath);
        }
        try {
            // TODO unsafe sql
            String sql = catalogBaseTable.getOptions().get("sr-createMeta");
            if (sql == null) {
                sql = toCreateTableSql(tablePath, catalogBaseTable);
            }
            executeSql(jdbcUrl + tablePath.getDatabaseName(), sql);
        } catch (UnsupportedDataTypeException e) {
            throw new CatalogException(e);
        }

    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        CatalogBaseTable oriCatalogTable = getTable(tablePath);
        SchemaChange schemaChange = SchemaChange.compare(oriCatalogTable.getUnresolvedSchema(), newCatalogTable.getUnresolvedSchema());

        try {
            String sql = "ALTER TABLE " + tablePath.getObjectName() + " " + schemaChange.toSchemaChangeSql() + ";";
            executeSql(jdbcUrl + tablePath.getDatabaseName(), sql);
        } catch (UnsupportedDataTypeException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return extractColumnMapValuesBySQL(
                jdbcUrl + tablePath.getDatabaseName(),
                "SHOW PARTITIONS FROM " + tablePath.getObjectName()
        ).stream().map(CatalogPartitionSpec::new).collect(Collectors.toList());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        if (!partitionSpec.getPartitionSpec().containsKey("PartitionName")) {
            return Collections.emptyList();
        }
        return extractColumnMapValuesBySQL(
                jdbcUrl + tablePath.getDatabaseName(),
                "SHOW PARTITIONS FROM " + tablePath.getObjectName() + "WHERE PartitionName = ?",
                partitionSpec.getPartitionSpec().get("PartitionName")
        ).stream().map(CatalogPartitionSpec::new).collect(Collectors.toList());
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> expressions) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return new CatalogPartitionImpl(partitionSpec.getPartitionSpec(), null);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        if (!partitionSpec.getPartitionSpec().containsKey("PartitionName")) {
            return false;
        }
        return !extractColumnMapValuesBySQL(
                jdbcUrl + tablePath.getDatabaseName(),
                "SHOW PARTITIONS FROM " + tablePath.getObjectName() + "WHERE PartitionName = ?",
                partitionSpec.getPartitionSpec().get("PartitionName")
        ).isEmpty();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {

    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException, TablePartitionedException {

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

    }

    private String getDatabaseVersion() {
        return extractColumnValuesBySQL(
                defaultUrl,
                "SELECT CURRENT_VERSION();",
                1,
                v -> true).get(0);
    }

    private String toCreateTableSql(ObjectPath tablePath, CatalogBaseTable catalogBaseTable) throws UnsupportedDataTypeException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(tablePath.getDatabaseName()).append("`.`").append(tablePath.getObjectName()).append("`(\n");
        Schema schema = catalogBaseTable.getUnresolvedSchema();
        boolean first = true;
        for (Schema.UnresolvedColumn column : schema.getColumns()) {
            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append("`").append(column.getName()).append("` ")
                        .append(StarRocksTypeMapper.mapping(((Schema.UnresolvedPhysicalColumn) column).getDataType()))
                        .append(" COMMENT \"").append(column.getComment().orElse("")).append("\"");
                sb.append("\n");
            }
        }
        sb.append(")\n");

        Map<String, String> options = catalogBaseTable.getOptions().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                entry -> entry.getKey().toUpperCase(),
                                Map.Entry::getValue
                        )
                );
        if (options.containsKey("ENGINE")) {
            sb.append("ENGINE=").append(options.get("ENGINE")).append("\n");
        }
        if (options.containsKey("PRIMARY KEY")) {
            sb.append("PRIMARY KEY ").append(options.get("PRIMARY KEY")).append("\n");
        } else if (catalogBaseTable.getUnresolvedSchema().getPrimaryKey().isPresent()){
            Schema.UnresolvedPrimaryKey primaryKey = catalogBaseTable.getUnresolvedSchema().getPrimaryKey().get();
            sb.append(String.format("PRIMARY KEY (%s)", primaryKey.getColumnNames().stream().map(EncodingUtils::escapeIdentifier).collect(Collectors.joining(", ")))).append("\n");
        }
        if (options.containsKey("PARTITION_DESC")) {
            sb.append(options.get("PARTITION_DESC")).append("\n");
        }
        if (options.containsKey("DISTRIBUTED_DESC")) {
            sb.append(options.get("DISTRIBUTED_DESC")).append("\n");
        }
        if (options.containsKey("PROPERTIES")) {
            sb.append("PROPERTIES ").append(options.get("PROPERTIES"));
        }
        sb.append(";");
        return sb.toString();
    }

    protected List<String> extractColumnValuesBySQL(String url,
                                                    String sql,
                                                    int columnIndex,
                                                    Predicate<String> filterFunc,
                                                    Object... args) {
        List<String> columnValues = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(args) && args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(String.format("The following SQL query could not be executed (%s): %s", url, sql), e);
        }
    }

    protected List<Map<String, String>> extractColumnMapValuesBySQL(String url,
                                                                    String sql,
                                                                    Object... args) {
        List<Map<String, String>> columnValues = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(args) && args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(String.format("The following SQL query could not be executed (%s): %s", url, sql), e);
        }
    }

    protected void executeSql(String url, String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (Objects.nonNull(args) && args.length > 0) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[i]);
                }
            }
            ps.executeUpdate();
        } catch (Exception e) {
            throw new CatalogException(String.format("The following SQL query could not be executed (%s): %s", url, sql), e);
        }
    }

}
