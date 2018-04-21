package tunaiku.collector.core.options;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.ReadablePeriod;



public abstract class QueryArgs extends DatabaseArgs {
    int limit;
    DateTime partition;
    String partitionColumn;
    ReadablePeriod partitionPeriod;

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public DateTime getPartition() {
        return partition;
    }

    public void setPartition(DateTime partition) {
        this.partition = partition;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public ReadablePeriod getPartitionPeriod() {
        return partitionPeriod;
    }

    public void setPartitionPeriod(ReadablePeriod partitionPeriod) {
        this.partitionPeriod = partitionPeriod;
    }


    /*
        TO DO :  - parameter with Statement Preparator
                 - format date time manageable
     */
    public String buildQueries(){
        String limitQuery = "";
        String whereQuery = "";

        if(limit != 0) limitQuery = String.format("LIMIT %d",limit);

        if ((partition != null && partitionColumn != null) == true){
            LocalDateTime partitionDate = partition.toLocalDateTime();
            LocalDateTime nextPartitionDate = partitionDate.plus(partitionPeriod);
            whereQuery = String.format("WHERE %s >= '%s' AND %s < '%s' ",partitionColumn,partitionDate.toString().replaceAll("T"," "),partitionColumn,nextPartitionDate.toString().replaceAll("T"," "));
        }

        return String.format("SELECT * FROM %s %s %s",tableName,whereQuery,limitQuery);
    }
}
