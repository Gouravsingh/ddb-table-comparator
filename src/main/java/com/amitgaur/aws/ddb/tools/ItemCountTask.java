package com.amitgaur.aws.ddb.tools;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by agaur on 11/23/15.
 */
public class ItemCountTask implements Callable<Long> {

    private final DynamoDBMapper mapper;
    private final AmazonDynamoDBClient amazonDynamoDBClient;
    private final  String tableName;
    private final Integer totalSegments;
    private final Boolean isConsistentRead;
    private final Integer segmentNum;
    private final Integer itemLimit;
    private final AtomicLong numItems;
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ItemCountTask.class);

    public ItemCountTask(AmazonDynamoDBClient amazonDynamoDBClient, String tableName, Integer itemLimit, Integer totalSegments, Integer segmentNum, Boolean isConsistentsRead) {
        this.mapper = new DynamoDBMapper(amazonDynamoDBClient);
        this.amazonDynamoDBClient = amazonDynamoDBClient;
        this.tableName = tableName;
        this.totalSegments = totalSegments;
        this.segmentNum = segmentNum;
        this.itemLimit = itemLimit;
        this.isConsistentRead = isConsistentsRead;
        this.numItems = new AtomicLong();

    }

    @Override
    public Long call() {
        LOGGER.info("Running with data segment num " + segmentNum + " totalDone " + numItems);

        Map<String, AttributeValue> exclusiveStartKey1 = null;
        RateLimiter limiter = RateLimiter.create(itemLimit);
        ExecutorService parallelSegmentService = Executors.newFixedThreadPool(1);
        CompareScanTask.ScanResultDecorator firstRequest = new CompareScanTask.ScanResultDecorator(this.amazonDynamoDBClient,this.tableName, null, this.isConsistentRead, this.itemLimit, this.totalSegments, this.segmentNum);
        try {
            while (true) {
                limiter.acquire(itemLimit);
                ScanResult firstResult = parallelSegmentService.submit(firstRequest).get();

                if (firstResult == null){
                    throw new Exception("Unable to get results from future");
                }
                Collection<Map<String, AttributeValue>> item1 = firstResult.getItems();
                if (item1 != null) {

                    numItems.getAndAdd(item1.size());
                }
                exclusiveStartKey1 = firstResult.getLastEvaluatedKey();
                firstRequest.setExclusiveStartKey(exclusiveStartKey1);

                if (exclusiveStartKey1 == null) {
                    break;
                }

            }
        } catch (Exception e) {
            LOGGER.error("Error", e);
        }
        parallelSegmentService.shutdownNow();
        try{
            parallelSegmentService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException e){

        }
        return numItems.longValue();

    }




}
