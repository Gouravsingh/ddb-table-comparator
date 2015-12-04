package com.amitgaur.aws.ddb.tools;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.dynamodb.bootstrap.DynamoDBBootstrapWorker;
import com.amazonaws.dynamodb.bootstrap.DynamoDBConsumer;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Created by agaur on 11/17/15.
 */
public class TableCopy {


    private static final Logger LOGGER = LoggerFactory.getLogger(TableCopy.class);

    public static void main(String... args) {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
        client.setRegion(Region.getRegion(Regions.US_WEST_1));

        DynamoDBBootstrapWorker worker = null;

        try {
            // 100.0 read operations per second. 4 threads to scan the table.
            worker = new DynamoDBBootstrapWorker(client,
                    100.0, "todo", 4);
        } catch (NullReadCapacityException e) {
            LOGGER.error("The DynamoDB source table returned a null read capacity.", e);
            System.exit(1);
        }

        ExecutorService service = new ThreadPoolExecutor(8,8, 8, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(8)) {
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                if (t == null && r instanceof Future<?>) {
                    try {
                        Future<?> future = (Future<?>) r;
                        if (future.isDone()) {
                            future.get();
                        }
                    } catch (CancellationException ce) {
                        t = ce;
                    } catch (ExecutionException ee) {
                        t = ee.getCause();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); // ignore/reset
                    }
                }
                if (t != null) {
                    LOGGER.error("Exception" + t.toString());
                }
            }
        };
        // 50.0 write operations per second. 8 threads to scan the table.
        DynamoDBConsumer consumer = new DynamoDBConsumer(client, "todo1", 50.0, Executors.newFixedThreadPool(10));

        try {
            worker.pipe(consumer);
        } catch (ExecutionException e) {
            LOGGER.error("Encountered exception when executing transfer.", e);
            System.exit(1);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when executing transfer.", e);
            System.exit(1);
        }

        LOGGER.info("done");
    }
}
