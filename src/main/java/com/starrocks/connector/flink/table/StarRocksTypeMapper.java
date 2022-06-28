package com.starrocks.connector.flink.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.UnsupportedDataTypeException;

public class StarRocksTypeMapper {

    private static final Logger log = LoggerFactory.getLogger(StarRocksTypeMapper.class);

    private static final String STARROCKS_UNKNOWN = "unknown";

    // -------------------------number----------------------------
    private static final String STARROCKS_BIGINT = "bigint";
    private static final String STARROCKS_LARGEINT = "largeint";
    private static final String STARROCKS_SMALLINT = "smallint";
    private static final String STARROCKS_TINYINT = "tinyint";
    private static final String STARROCKS_BOOLEAN = "boolean";
    private static final String STARROCKS_DECIMAL = "decimal";
    private static final String STARROCKS_DOUBLE = "double";
    private static final String STARROCKS_FLOAT = "float";
    private static final String STARROCKS_INT = "int";

    // -------------------------string----------------------------
    private static final String STARROCKS_CHAR = "char";
    private static final String STARROCKS_VARCHAR = "varchar";
    private static final String STARROCKS_STRING = "string";
    private static final String STARROCKS_JSON = "json";

    // ------------------------------time-------------------------
    private static final String STARROCKS_DATE = "date";
    private static final String STARROCKS_DATETIME = "datetime";

    // ------------------------------blob-------------------------
    private static final String STARROCKS_BINARY = "binary";

    // -----------------------------other-------------------------

    private static final String STARROCKS_ARRAY = "array";

    private static final TypeInfoLogicalTypeVisitor typeVisitor = new TypeInfoLogicalTypeVisitor();

    public static DataType mapping(String typeName, String columnSize, String scale) {
        typeName = typeName.toLowerCase();

        switch (typeName) {
            case STARROCKS_LARGEINT:
            case STARROCKS_BIGINT:
                return DataTypes.BIGINT();
            case STARROCKS_TINYINT:
                return DataTypes.TINYINT();
            case STARROCKS_SMALLINT:
                return DataTypes.SMALLINT();
            case STARROCKS_BOOLEAN:
                return DataTypes.BOOLEAN();
            case STARROCKS_DECIMAL:
                return DataTypes.DECIMAL(Integer.parseInt(columnSize), Integer.parseInt(scale));
            case STARROCKS_DOUBLE:
                return DataTypes.DOUBLE();
            case STARROCKS_FLOAT:
                return DataTypes.FLOAT();
            case STARROCKS_INT:
                return DataTypes.INT();
            case STARROCKS_CHAR:
                return DataTypes.CHAR(Integer.parseInt(columnSize));
            case STARROCKS_VARCHAR:
                return DataTypes.VARCHAR(Integer.parseInt(columnSize));
            case STARROCKS_STRING:
            case STARROCKS_JSON:
                return DataTypes.STRING();
            case STARROCKS_DATE:
                return DataTypes.DATE();
            case STARROCKS_DATETIME:
                return DataTypes.TIMESTAMP();
            default:
                return DataTypes.ROW();
        }
    }

    public static String mapping(AbstractDataType<?> dataType) throws UnsupportedDataTypeException {
        if (dataType instanceof DataType) {
            return ((DataType) dataType).getLogicalType().accept(typeVisitor);
        }
        throw new UnsupportedDataTypeException(String.format("Unsupported dataType(`%s`) to StarRocks", dataType.toString()));
    }

    private static class TypeInfoLogicalTypeVisitor extends LogicalTypeDefaultVisitor<String> {

        @Override
        protected String defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(String.format("Doesn't support converting type %s to StarRocks type yet.", logicalType.toString()));
        }

        @Override
        public String visit(CharType charType) {
            if (charType.getLength() > 255) {
                return "STRING";
            }
            return charType.asSerializableString();
        }

        @Override
        public String visit(VarCharType varCharType) {
            if (varCharType.getLength() > 1048576) {
                return "STRING";
            }
            return varCharType.asSerializableString();
        }

        @Override
        public String visit(BooleanType booleanType) {
            return booleanType.asSerializableString();
        }

        @Override
        public String visit(DecimalType decimalType) {
            return decimalType.asSerializableString();
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return smallIntType.asSerializableString();
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return tinyIntType.asSerializableString();
        }

        @Override
        public String visit(IntType intType) {
            return intType.asSerializableString();
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return bigIntType.asSerializableString();
        }

        @Override
        public String visit(FloatType floatType) {
            return floatType.asSerializableString();
        }

        @Override
        public String visit(DoubleType doubleType) {
            return doubleType.asSerializableString();
        }

        @Override
        public String visit(DateType dateType) {
            return dateType.asSerializableString();
        }

        @Override
        public String visit(TimestampType timestampType) {
            return STARROCKS_DATETIME;
        }

        @Override
        public String visit(ZonedTimestampType zonedTimestampType) {
            return STARROCKS_DATETIME;
        }

        @Override
        public String visit(LocalZonedTimestampType localZonedTimestampType) {
            return STARROCKS_DATETIME;
        }

        @Override
        public String visit(RowType rowType) {
            return STARROCKS_JSON;
        }
    }
}
