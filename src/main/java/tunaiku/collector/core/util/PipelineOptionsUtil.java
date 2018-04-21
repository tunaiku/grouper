package tunaiku.collector.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tunaiku.collector.core.options.CollectorPipelineOptions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class PipelineOptionsUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineOptionsUtil.class);

    public String readPasswordFile(CollectorPipelineOptions options){

        StringBuilder contentBuilder = new StringBuilder();

        if(options.getPasswordFile() != null){
            try (Stream<String> stream = Files.lines( Paths.get(options.getPasswordFile()), StandardCharsets.UTF_8))
            {
                stream.forEach(s -> contentBuilder.append(s).append("\n"));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            return contentBuilder.toString();
        }else{
            return options.getPassword();
        }

    }

}
