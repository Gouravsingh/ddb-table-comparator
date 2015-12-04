# Table Comparator
This is a sample library that does a deep compare to  2 dynamodb tables
Utilizes the Parallel Scan API

- The tool assumes some defaults for parallel scan operations but they can be configured from the command (see options below) 
- Requires the region and table information for each table to be compared. Output is logged to logs/application.log file : can be configured using log4j.xml


  -
### Version
0.1.0

### Tech

Look at pom.xml to see dependencies



### Installation and Dependencies
- You need Java 1.8 and Maven installed. 
- You also need to be able to access the Dynamodb endpoints from the resource you are going to be running this jar from.
- Currently the code uses AWS Default Identity Provider Chain for ensuring access to dynamodb tables  (http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html)


```sh
$ git clone [git-repo-url] tablecomparator
$ cd tablecomparator
$ mvn clean install 
$ mkdir -p logs/
$ java -cp target/ddb-sample-tool-0.1.0-jar-with-dependencies.jar com.amitgaur.aws.ddb.tools.TableComparator --help
```
### Sample Usage 
```sh
java -cp ddb-sample-tool-0.1.0-jar-with-dependencies.jar com.amitgaur.aws.ddb.tools.TableComparator --region1 eu-west-1 --region2 eu-west-1 --table1 64Parts2Host --table2 64Parts2HostRestore &
```
### Options Configurability
- httpTimeout
- segments : Number of parallel segment scans
- itemLimit : Items to be fetched per fetch : rate limiting mechanism on DDB
- maxCons : Max number of http connections
- region1 and table1 : region end point and table name for first table
- region2 and table2 : region end point and table name for second table

### Logs
Logs are created in the logs directory in a file called application.log


### Development

Want to contribute? Great!



### Todos

 - Write Tests
 - Add Code Comments
 - Add Sample Mode





