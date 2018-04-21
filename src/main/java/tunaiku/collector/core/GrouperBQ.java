package tunaiku.collector.core;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tunaiku.collector.core.options.CollectorArgs;
import tunaiku.collector.core.options.CollectorExportArgs;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GrouperBQ {
    private static final Logger LOG = LoggerFactory.getLogger(GrouperBQ.class);

    public static TableSchema createSchemaBigQuery(CollectorArgs collector) throws SQLException {
        Long startTimeMillis = System.currentTimeMillis();
        Connection connection = collector.createConnection();
        TableSchema schema = new TableSchema();
        schema.setFields(new GrouperConvertions().createSchemaByReadingOneRow(connection,collector.getTableName()));
        Long elapsedTimeSchema = System.currentTimeMillis() - startTimeMillis;
        LOG.info(String.format("Elapsed time to schema %f seconds",(elapsedTimeSchema / 1000.0)));
        return schema;
    }

    public static class ResultSetGenericRecordMapper implements JdbcIO.RowMapper<GrouperRow> {

        @Override
        public GrouperRow mapRow(ResultSet resultSet) throws Exception {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();
            List<Object> values = new ArrayList<Object>();
            for (int column = 1; column <= columnCount; ++column)
            {
                String name = meta.getColumnName(column);
                Object value = resultSet.getObject(name);
                values.add(value);
            }
            return GrouperRow.create(values);
        }
    }

    static void runCollectorJdbc(CollectorArgs collector) throws SQLException {
        Pipeline p = Pipeline.create(collector.getOptions());
        TableSchema bqJdbcSchema = createSchemaBigQuery(collector);
        List<String> columnNames = new ArrayList<String>();
        String tableNameBQ = collector.getOutputTable() != null ? collector.getOutputTable() : collector.getTableName();

        for(TableFieldSchema fieldSchema : bqJdbcSchema.getFields())
        {
            columnNames.add(fieldSchema.getName());
        }


        p
                .apply(JdbcIO.<GrouperRow>read()
                    .withCoder(SerializableCoder.of(GrouperRow.class))
                    .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                            collector.getDriverClass(), collector.getConnectionUrl())
                            .withUsername(collector.getUsername())
                            .withPassword(collector.getPassword()))
                    .withQuery(collector.buildQueries())
                    .withRowMapper(new ResultSetGenericRecordMapper()))
                .apply("Jdbc row to BigQuery TableRow", ParDo.of(new DoFn<GrouperRow,TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        GrouperRow data = c.element();
                        List<String> columnList = columnNames;
                        List<Object> fields = data.fields();
                        TableRow row = new TableRow();
                        for(int i = 0; i < fields.size(); i++){
                            Object fieldData = fields.get(i);
                            String columnName = columnList.get(i);
                            if(fieldData == null) continue;
                            String sFieldData = fieldData.toString();
                            if(!sFieldData.toLowerCase().equals("null")) row.put(columnName, sFieldData);
                        }
                        c.output(row);
                    }

                    }))
                .setCoder(TableRowJsonCoder.of())
                .apply(BigQueryIO.writeTableRows().to(collector.getOutput() + "." + tableNameBQ)
                        .withSchema(bqJdbcSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }


    public static void main(String args[]){
        CollectorArgs collectorOptions = new CollectorExportArgs().contextAndArgs(args);
        try {
            runCollectorJdbc(collectorOptions);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
