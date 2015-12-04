package com.amitgaur.aws.ddb.tools;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Created by agaur on 11/25/15.
 */
public class Configuration{
    @Parameter(names = "--httpTimeout", description = "http connection timeout")
    public int connectionTimeout = 10000;
    @Parameter(names = "--segments", description = "number of parallel segment scans")
    public int concurrentTaskNum = 100;
    @Parameter(names = "--region1", description = "Region for first table http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html eg:  us-west-1", required = true)
    public String region1;
    @Parameter(names = "--table1", description = "first table name", required = true)
    public String table1;
    @Parameter(names = "--region2", description = "Region for second table http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html eg: us-west-2", required = true)
    public String region2;
    @Parameter(names = "--table2", description = "second table name", required = true)
    public String table2;
    @Parameter(names = "--maxCons", description = "Max Http Connections per client")
    public int maxConns=1000;
    @Parameter(names = "--itemLimit", description = "Max Item Limit per fetch")
    public Integer itemLimit=500;
    @Parameter(names = "--help", help = true)
    private boolean help = false;


    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }




    public static Configuration build(String... args) {
        Configuration example = new Configuration();


       JCommander jCommander=  new JCommander(example, args);
        jCommander.setProgramName("TableComparator");
        if (example.help) {
            jCommander.usage();
            return null;
        }
        return example;



    }
}
