package tunaiku.collector.core.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;

public interface JdbcCollectPipelineOptions extends CollectorPipelineOptions{
    @Description("The date of the current partition.")
    String getPartition();
    void setPartition(String value);

    @Description("The name of a date/timestamp column to filter data based on current partition.")
    String getPartitionColumn();
    void setPartitionColumn(String value);

    @Description("By default, when partition column is not specified, " +
            "fails if partition is too old. Set this flag to ignore this check.")
    @Default.Boolean(false)
    boolean isSkipPartitionCheck();
    void setSkipPartitionCheck(boolean value);

    @Description("The minimum partition required for the job not to fail " +
            "(when partition column is not specified), by default `now() - 2*partitionPeriod`.")
    String getPartitionPeriod();
    void setPartitionPeriod(String value);

    String getMinPartitionPeriod();

    void setMinPartitionPeriod(String value);

    @Description("Limit the output number of rows, indefinite by default.")
    Integer getLimit();

    void setLimit(Integer value);
}

