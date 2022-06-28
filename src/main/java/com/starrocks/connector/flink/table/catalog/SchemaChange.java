package com.starrocks.connector.flink.table.catalog;

import com.starrocks.connector.flink.table.StarRocksTypeMapper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Schema;

import javax.activation.UnsupportedDataTypeException;
import java.util.List;
import java.util.Objects;

public class SchemaChange {

    private List<Schema.UnresolvedColumn> addChange;
    private List<Schema.UnresolvedColumn> dropChange;
    private List<Tuple2<Schema.UnresolvedColumn, Schema.UnresolvedColumn>> updateChange;

    public List<Schema.UnresolvedColumn> getAddChange() {
        return addChange;
    }

    public void setAddChange(List<Schema.UnresolvedColumn> addChange) {
        this.addChange = addChange;
    }

    public List<Schema.UnresolvedColumn> getDropChange() {
        return dropChange;
    }

    public void setDropChange(List<Schema.UnresolvedColumn> dropChange) {
        this.dropChange = dropChange;
    }

    public List<Tuple2<Schema.UnresolvedColumn, Schema.UnresolvedColumn>> getUpdateChange() {
        return updateChange;
    }

    public void setUpdateChange(List<Tuple2<Schema.UnresolvedColumn, Schema.UnresolvedColumn>> updateChange) {
        this.updateChange = updateChange;
    }

    public String toSchemaChangeSql() throws UnsupportedDataTypeException {
        StringBuilder sb = new StringBuilder();
        for (Schema.UnresolvedColumn column : addChange) {
            Schema.UnresolvedPhysicalColumn physicalColumn = (Schema.UnresolvedPhysicalColumn) column;
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("ADD COLUMN `").append(physicalColumn.getName()).append("` ")
                    .append(StarRocksTypeMapper.mapping(physicalColumn.getDataType()))
                    .append(" COMMENT \"").append(physicalColumn.getComment().orElse("")).append("\"");
        }

        return sb.toString();
    }

    public static SchemaChange compare(Schema oriSchema, Schema newSchema) throws UnsupportedOperationException {
        List<Schema.UnresolvedColumn> oriColumns = oriSchema.getColumns();
        List<Schema.UnresolvedColumn> newColumns = newSchema.getColumns();

        if (oriColumns.size() > newColumns.size()) {
            throw new UnsupportedOperationException("Unsupported remove columns");
        }

        for (int i = 0; i < oriColumns.size(); i++) {
            if (!Objects.equals(oriColumns.get(i).getName(), newColumns.get(i).getName())) {
                throw new UnsupportedOperationException("Unsupported rename columns");
            }
        }

        SchemaChange schemaChange = new SchemaChange();
        schemaChange.setAddChange(newColumns.subList(oriColumns.size(), newColumns.size()));

        return schemaChange;
    }

}
