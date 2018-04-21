package tunaiku.collector.core.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.DateTime;
import org.joda.time.ReadablePeriod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CollectorArgs extends QueryArgs {
    String driverClass;
    String connectionUrl;
    String username;
    String password;
    String output;
    String outputTable;
    PipelineOptions options;

    public PipelineOptions getOptions() {
        return options;
    }

    public void setOptions(PipelineOptions options) {
        this.options = options;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOutputTable() {
        return outputTable;
    }

    public void setOutputTable(String outputTable) {
        this.outputTable = outputTable;
    }

    public CollectorArgs(String output, String driverClass, String connectionUrl, String username, String password, String tableName, Integer limit, String partitionColumn, DateTime partition, ReadablePeriod partitionPeriod,String outputTable, PipelineOptions options) {
        this.driverClass = driverClass;
        this.connectionUrl = connectionUrl;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.limit = limit;
        this.partitionColumn = partitionColumn;
        this.partition = partition;
        this.partitionPeriod = partitionPeriod;
        this.output = output;
        this.outputTable = outputTable;
        this.options = options;

    }

    public Connection createConnection(){
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(connectionUrl,username,password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
