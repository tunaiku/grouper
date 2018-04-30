package tunaiku.collector.core.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Period;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tunaiku.collector.core.util.JdbcConnectionUtil;
import tunaiku.collector.core.util.PipelineOptionsUtil;

public class CollectorExportArgs {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorExportArgs.class);

    public DateTime validatePartition(DateTime partitionDateTime,DateTime minPartiotionDateTime){
        if(!partitionDateTime.isAfter(minPartiotionDateTime)){
            LOG.error(String.format("Too old partition date %s. Use a partition date >= %s or use --skip-partition-check",partitionDateTime,minPartiotionDateTime));
            return null;
        }
        return partitionDateTime;
    }

    public DateTime parseDateTime(String input){
        String pattern = "yyyy-MM-dd HH:mm:ss";
        return DateTime.parse(input, DateTimeFormat.forPattern(pattern));
    }

    public CollectorArgs fromPipelineOptions(PipelineOptions options){
        JdbcCollectPipelineOptions exportOptions = options.as(JdbcCollectPipelineOptions.class);
        PeriodFormatter formatter = new PeriodFormatterBuilder()
                .appendWeeks().appendSuffix("w ")
                .appendDays().appendSuffix("d ")
                .appendHours().appendSuffix("h ")
                .appendMinutes().appendSuffix("min")
                .toFormatter();
        ReadablePeriod partitionPeriod = exportOptions.getPartitionPeriod() == null ?  Days.ONE : Period.parse(exportOptions.getPartitionPeriod(),formatter);
        String partitionColumn = exportOptions.getPartitionColumn();
        boolean skipPartitionCheck = exportOptions.isSkipPartitionCheck();
        DateTime partition = exportOptions.getPartition() != null ? parseDateTime(exportOptions.getPartition()) : null;

        JdbcConnectionUtil jdbcHelper = new JdbcConnectionUtil();
        PipelineOptionsUtil pipelineHelper = new PipelineOptionsUtil();

        if(exportOptions.getConnectionUrl() == null){
            LOG.error(String.format("'connectionUrl' must be defined"));
            return null;
        }

        if(exportOptions.getTable() == null){
            LOG.error(String.format("'table' must be defined"));
            return null;
        }

        if( !skipPartitionCheck && partitionColumn.isEmpty()){
            DateTime minPartitionDatetime = parseDateTime(exportOptions.getMinPartitionPeriod()) != null ? parseDateTime(exportOptions.getMinPartitionPeriod()) : DateTime.now().minus(partitionPeriod.toPeriod().multipliedBy(2));
            partition = validatePartition(partition,minPartitionDatetime);
        }


        return new CollectorArgs(
                exportOptions.getOutput(),
                jdbcHelper.getDriverClass(exportOptions.getConnectionUrl()),
                exportOptions.getConnectionUrl(),
                exportOptions.getUsername(),
                pipelineHelper.readPasswordFile(exportOptions),
                exportOptions.getTable(),
                exportOptions.getLimit() == null ? 0 : exportOptions.getLimit() ,
                partitionColumn,
                partition,
                partitionPeriod,
                exportOptions.getOutputTable(),
                options
                );
    }

    public CollectorArgs contextAndArgs(String cmdLineArgs[]){
        PipelineOptionsFactory.register(JdbcCollectPipelineOptions.class);
        PipelineOptionsFactory.register(CollectorPipelineOptions.class);
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(cmdLineArgs).withValidation().create();
        return fromPipelineOptions(opts);
    }

}
