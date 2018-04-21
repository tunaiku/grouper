package tunaiku.collector.core.options;

public abstract class DatabaseArgs {
    private static final String regexTable = "^[a-zA-Z_][a-zA-Z0-9_]*$";

    String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean checkTableName(){
        return this.tableName.matches(regexTable);
    }


}
