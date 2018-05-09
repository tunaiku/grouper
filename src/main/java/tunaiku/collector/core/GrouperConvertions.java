package tunaiku.collector.core;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.*;

public class GrouperConvertions {
    private static final Logger LOG = LoggerFactory.getLogger(GrouperConvertions.class);

    private static final int MAX_DIGITS_BIGINT = 19;
    private static final Map<String, String> jdbcToBqTypeMap = ImmutableMap.<String, String>builder()
            .put("NVARCHAR", "STRING")
            .put("LONGVARCHAR", "STRING")
            .put("VARCHAR", "STRING")
            .put("CHAR", "STRING")
            .put("ALPHANUM", "STRING")
            .put("SHORTTEXT", "STRING")
            .put("BLOB", "STRING")
            .put("CLOB", "STRING")
            .put("NCLOB", "STRING")
            .put("TEXT", "STRING")
            .put("VARBINARY", "BYTES")
            .put("INTEGER", "INTEGER")
            .put("DATE", "DATE")
            .put("TIME", "TIME")
            .put("DATETIME", "DATETIME")
            .put("TIMESTAMP", "DATETIME")
            .put("BOOLEAN", "BOOLEAN")
            .put("TINYINT", "INTEGER")
            .put("SMALLINT", "INTEGER")
            .put("BIGINT", "INTEGER")
            .put("SMALLDECIMAL", "FLOAT")
            .put("DECIMAL", "FLOAT")
            .put("DOUBLE", "FLOAT")
            .put("REAL", "FLOAT")
            .put("BIT","INTEGER")
            .build();

    /**
     * Fetch resultSet data and convert to Java Objects
     * org.postgresql.jdbc.TypeInfoCache
     * com.mysql.jdbc.MysqlDefs#mysqlToJavaType(int)
     */
    public Object convertFieldToType(ResultSet r,ResultSetMetaData m,int i) throws SQLException {
        Object generateObject = new Object();

       if(m.getColumnType(i) == CHAR || m.getColumnType(i) == CLOB || m.getColumnType(i) == LONGNVARCHAR ||
        m.getColumnType(i) == LONGVARCHAR || m.getColumnType(i) == NCHAR || m.getColumnType(i) == NVARCHAR
               || m.getColumnType(i) == VARCHAR){
           generateObject = r.getString(i);
       }else if(m.getColumnType(i) == BOOLEAN){
           generateObject = r.getBoolean(i);
       }else if(m.getColumnType(i) == BINARY || m.getColumnType(i) == VARBINARY || m.getColumnType(i) == LONGVARBINARY ||
               m.getColumnType(i) == ARRAY || m.getColumnType(i) == BLOB){
           generateObject =  nullableBytes(r.getBytes(i));
       }else if(m.getColumnType(i) == TINYINT || m.getColumnType(i) == SMALLINT || m.getColumnType(i) == INTEGER){
           generateObject = r.getInt(i);
       }else if(m.getColumnType(i) == FLOAT || m.getColumnType(i) ==  REAL){
           generateObject = r.getFloat(i);
       }else if(m.getColumnType(i) == DOUBLE){
           generateObject = r.getFloat(i);
       }else if(m.getColumnType(i) == DATE || m.getColumnType(i) == TIME || m.getColumnType(i) == TIMESTAMP ||
               m.getColumnType(i) == TIMESTAMP_WITH_TIMEZONE){
           Timestamp t = r.getTimestamp(i);
           if (t != null) {
               generateObject =  t.getTime();
           } else {
               generateObject = t;
           }
       }else if(m.getColumnType(i) == BIT){
            int precision = m.getPrecision(i);
           if (precision <= 1) {
              generateObject =  r.getBoolean(i);
           } else {
               generateObject = nullableBytes(r.getBytes(i));
           }
       }else if(m.getColumnType(i) == BIGINT){
           int precision = m.getPrecision(i);
           if (precision > 0 && precision <= MAX_DIGITS_BIGINT) {
               generateObject = r.getLong(i);
           } else {
               generateObject = r.getString(i);
           }
       }else{
           generateObject = r.getString(i);
       }

       return r != null ? generateObject : null;
    }

    private ByteBuffer nullableBytes(byte[] bts) {
        if (bts != null) {
           return ByteBuffer.wrap(bts);
        } else {
           return null;
        }
    }

    public List<Object> convertResultSetIntoJdbcRecord(ResultSet result) throws SQLException {
        List<Object> resultField = new ArrayList<Object>();
        ResultSetMetaData meta = result.getMetaData();

        for (int i=1;i<=result.getFetchSize();i++){
            Object schemaValue = convertFieldToType(result,meta,i);
            if(resultField != null){
                resultField.add(schemaValue);
            }
        }

        return resultField;
    }

    String normalizeForConvertion(String input) {
        return input.replaceAll("[^A-Za-z0-9_]", "_");
    }

    public List<TableFieldSchema> createSchemaByReadingOneRow (Connection connection, String tableName) throws SQLException {
        LOG.info("Creating BigQuery schema based on the first read row from the database");
        try {
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(String.format("select * from %s limit 1",tableName));
            List<TableFieldSchema> schemaField = createBigQuerySchema(result.getMetaData());
            LOG.info("Schema created successfully. Generated schema: "+schemaField.toString());
            return schemaField;

        } catch (SQLException e) {

            e.printStackTrace();

        }finally {
            if (connection != null) {
                connection.close();
            }
        }
        return null;
    }

    public List<TableFieldSchema> createBigQuerySchema(ResultSetMetaData meta) throws SQLException {
        List<TableFieldSchema> fields = new ArrayList<>();

        Map<String, String> columns = new HashMap<String,String>();
        List<String> orderedColumns = new ArrayList<String>();

        for(int i=1;i<=meta.getColumnCount();i++){
            TableFieldSchema schemaEntry = new TableFieldSchema();
            String columnName = meta.getColumnName(i).isEmpty() ? meta.getColumnLabel(i) : meta.getColumnName(i);
            String normalizeColumnName = normalizeForConvertion(columnName);
            int columnType = meta.getColumnType(i);
            String typeName = JDBCType.valueOf(columnType).getName();
            schemaEntry.setName(normalizeColumnName);
            if(jdbcToBqTypeMap.containsKey(typeName))
                schemaEntry.setType(jdbcToBqTypeMap.get(typeName));
            else {
                LOG.error("Unhandled JDBC type: " +  typeName);
                throw new SQLException("Unhandled JDBC Type");
            }


            fields.add(schemaEntry);
        }
        return fields;
    }

}
