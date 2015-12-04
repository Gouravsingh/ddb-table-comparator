Tool to compare 2 dynamodb tables
Implementation does a parallel scan and deep compare of attributes returned
The tool assumes some defaults for parallel scan operations but they can be configured from the command (see options below)
Requires the region and table information for each table to be compared.
Output is logged to logs/application.log file : can be configured using log4j.xml 


Installation Steps on EC2

1.Checkout the code
2. mvn clean install
3. Usage

java -cp target/ddb-sample-tool-0.1.0-jar-with-dependencies.jar com.amitgaur.aws.ddb.tools.TableComparator  --help
Usage: TableComparator [options]
Options:
--help

Default: false
--httpTimeout
http connection timeout
Default: 10000
--itemLimit
Max Item Limit per fetch
Default: 500
--maxCons
Max Http Connections per client
Default: 1000
* --region1
Region for first table http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html
eg:  us-west-1
* --region2
Region for second table http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html
eg: us-west-2
--segments
number of parallel segment scans
Default: 100
* --table1
first table name
* --table2
second table name

