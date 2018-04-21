package tunaiku.collector.core.options;

import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;



public interface CollectorPipelineOptions extends PipelineOptions {
    @Description("The path for storing the output.")
    @Required
    String getOutput();

    void setOutput(String value);

    @Description("Table name Output to storing in destination table")
    String getOutputTable();
    void setOutputTable(String value);


    @Description("The JDBC connection url to perform the extraction on.")
    @Required
    String getConnectionUrl();
    void setConnectionUrl(String value);

    @Description("The database table to query and perform the extraction on.")
    @Required
    String getTable();
    void setTable(String value);

    @Description("The database user name used by JDBC to authenticate.")
    String getUsername();
    void setUsername(String value);

    @Description("A path to a local file containing the database password.")
    String getPasswordFile();
    void setPasswordFile(String value);


    @Description("Database password")
    String getPassword();
    void setPassword(String value);

}





