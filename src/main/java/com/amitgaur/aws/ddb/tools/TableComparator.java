package com.amitgaur.aws.ddb.tools;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by agaur on 11/16/15.
 */
public class TableComparator {
    private static final org.slf4j.Logger LOGGER= LoggerFactory.getLogger(TableComparator.class);


    /**
     * The only information needed to create a client are security credentials -
     * your AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints have defaults provided.
     *
     * Additional client parameters, such as proxy configuration, can be specified
     * in an optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static AWSCredentialsProvider init() throws Exception {



        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        return credentialsProvider;

    }
    public static void compareTables(Pair<AmazonDynamoDBClient, String> first, Pair<AmazonDynamoDBClient, String> second, Configuration configuration){


        DescribeTableResult result1=  first.getLeft().describeTable(new DescribeTableRequest().withTableName(first.getRight()));
        DescribeTableResult result2 =  second.getLeft().describeTable(new DescribeTableRequest().withTableName(second.getRight()));
        if (result1 == null || result2 == null){
            LOGGER.error("Tables dont exist, please verify your arguments and make sure tables exist");
            System.exit(1);
        }
        if (result1.getTable().getItemCount() != result2.getTable().getItemCount() || result1.getTable().getTableSizeBytes()!= result2.getTable().getTableSizeBytes()){
            LOGGER.error("Mismatch in meta data on item size or item bytes table1 : " + result1.getTable().getItemCount() + result1.getTable().getTableSizeBytes()  + " table2 : " + result2.getTable().getItemCount() + result2.getTable().getTableSizeBytes());

        } else {
            LOGGER.info("Item count between tables is a match!");
        }
        int concurrentTasks = configuration.concurrentTaskNum;
        ExecutorService service = Executors.newFixedThreadPool(concurrentTasks);
        CompletionService<TableCompareResult> taskCompletionService =
                new ExecutorCompletionService<>(service);

        AtomicLong numCompares = new AtomicLong();
        AtomicLong  equalCount = new AtomicLong();
        AtomicLong notEqualCount = new AtomicLong();
         for (int i = 0 ; i < concurrentTasks; i ++){

            taskCompletionService.submit(new CompareScanTask(first,second,configuration.itemLimit, concurrentTasks, i, Boolean.TRUE));
        }

        TableCompareResult tableCompareResult = null;

        for (int i = 0 ; i < concurrentTasks; i ++){
            try {

                Future<TableCompareResult> result = taskCompletionService.take();
                // above call blocks till atleast one task is completed and results available for it
                // but we dont have to worry which one


                tableCompareResult = result.get();
                numCompares.getAndAdd(tableCompareResult.numCompares);
                equalCount.getAndAdd(tableCompareResult.equals);
                notEqualCount.getAndAdd(tableCompareResult.notEquals);


            } catch (InterruptedException e) {
                // Something went wrong with a task submitted
               LOGGER.error(e.getStackTrace().toString());
               return;
            } catch (ExecutionException e) {
                // Something went wrong with the result
                LOGGER.error(e.getStackTrace().toString());
               return;
            }

        }
        service.shutdownNow();
        LOGGER.info("Final Result Number of items compared " + numCompares + " equals" + equalCount + " notEq" + notEqualCount  );
        if (equalCount.longValue()!=numCompares.longValue()){
            LOGGER.info("Items dont match between the 2 tables");
        }

    }

    public static void getItemCount(AmazonDynamoDBClient amazonDynamoDBClient, String table, int concurrentTasks){
        ExecutorService service = Executors.newFixedThreadPool(concurrentTasks);
        Collection<Future<Long>> results = new ArrayList<Future<Long>>(concurrentTasks);
        for (int i = 0 ; i < concurrentTasks; i ++){

            results.add(service.submit(new ItemCountTask(amazonDynamoDBClient,table,750, concurrentTasks, i, Boolean.TRUE)));
        }

        Long total = 0L;
        try{
            for (Future<Long> taskResult : results){
                total+= taskResult.get().longValue();
            }
        }
        catch (InterruptedException e) {
            LOGGER.error("error evaulating result", e);
        } catch (ExecutionException e) {
            LOGGER.error("error evaulating result", e);
        }

    }
    public static void main(String... args) throws Exception{
        Configuration configuration = Configuration.build(args);
        if (configuration == null){
            System.exit(0);
        }
        try {

            AWSCredentialsProvider credentialsProvider = init();
            ClientConfiguration clientConfig = new ClientConfiguration().withConnectionTimeout(configuration.connectionTimeout).withTcpKeepAlive(true).withMaxConnections(configuration.maxConns);
            AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient( credentialsProvider, clientConfig);
            amazonDynamoDBClient.setRegion(Region.getRegion(Regions.fromName(configuration.region1)));
            Pair<AmazonDynamoDBClient,String> first = new ImmutablePair<>(amazonDynamoDBClient, configuration.table1);


            AmazonDynamoDBClient amazonDynamoDBClient2 = new AmazonDynamoDBClient( credentialsProvider, clientConfig);
            amazonDynamoDBClient2.setRegion(Region.getRegion(Regions.fromName(configuration.region2)));

            Pair<AmazonDynamoDBClient,String> second = new ImmutablePair<>(amazonDynamoDBClient, configuration.table2);


            Long timeNow = System.currentTimeMillis();

            compareTables(first,second,configuration);
            Long now = System.currentTimeMillis();
            LOGGER.info("Total time taken"   +(now-timeNow) + " ms");

        } catch (AmazonServiceException ase) {
            /*
             * AmazonServiceExceptions represent an error response from an AWS
             * services, i.e. your request made it to AWS, but the AWS service
             * either found it invalid or encountered an error trying to execute
             * it.
             */
            LOGGER.error("Error Message:    " + ase.getMessage());
            LOGGER.error("HTTP Status Code: " + ase.getStatusCode());
            LOGGER.error("AWS Error Code:   " + ase.getErrorCode());
            LOGGER.error("Error Type:       " + ase.getErrorType());
            LOGGER.error("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            /*
             * AmazonClientExceptions represent an error that occurred inside
             * the client on the local host, either while trying to send the
             * request to AWS or interpret the response. For example, if no
             * network connection is available, the client won't be able to
             * connect to AWS to execute a request and will throw an
             * AmazonClientException.
             */
            if (ace.isRetryable()){

            }
            LOGGER.error("Error Message: " + ace.getMessage());
        }



    }



}
