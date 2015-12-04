package com.amitgaur.aws.ddb.tools;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationTest{

    @Test
    public void testAllArgs(){
        Configuration example = new Configuration();
        String[] argv = {"--httpTimeout", "2","--maxCons","1000", "--segments", "1000", "--region1", "us-west-1", "--table1", "Backup", "--region2", "us-west-1", "--table2",
                "Backup1",};
        new JCommander(example, argv);
        Assert.assertEquals(example.connectionTimeout, 2);

    }
}
