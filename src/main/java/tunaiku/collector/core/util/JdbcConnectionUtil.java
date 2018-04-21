package tunaiku.collector.core.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcConnectionUtil {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionUtil.class);

    private static final HashMap<String, String> driverMapping = new HashMap<String, String>() {{
        put("postgresql","org.postgresql.Driver");
        put("mysql" , "com.mysql.jdbc.Driver");
        put("h2" , "org.h2.Driver");
    }};

    public String getDriverClass(String url){
        List<String> partsUrl = Arrays.asList(url.split(":"));
        String driverVal = null;
        if(!partsUrl.get(0).equals("jdbc"))
            LOG.error("Invalid jdbc connection URL: {} Expect jdbc:postgresql or jdbc:mysql as prefix.",url);

        if(driverMapping.containsKey(partsUrl.get(1)))
            driverVal = driverMapping.get(partsUrl.get(1));
        else
            LOG.error("Invalid jdbc connection URL: {} Expect jdbc:postgresql or jdbc:mysql as prefix.",url);


        return driverVal;
    }

    public JdbcIO.RowMapper createFieldJdbc(){

        return null;
    }
}
