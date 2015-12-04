package com.amitgaur.aws.ddb.tools;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by agaur on 11/17/15.
 */

public class CompareScanTask implements Callable<TableCompareResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompareScanTask.class);
    private final Pair<AmazonDynamoDBClient,String> first;
    private final Pair<AmazonDynamoDBClient, String> second;
    private final Integer totalSegments;
    private final Integer segmentNum;
    private int itemLimit;
    private final Boolean isConsistentRead;
    private final AtomicLong numCompares;
    private final AtomicLong equals;
    private final AtomicLong notEquals;

    /**
     *
     * @param first
     * @param second
     * @param itemLimit
     * @param totalSegments
     * @param segmentNum
     * @param isConsistentsRead
     */
    public CompareScanTask(Pair<AmazonDynamoDBClient,String> first, Pair<AmazonDynamoDBClient,String> second, Integer itemLimit, Integer totalSegments, Integer segmentNum, Boolean isConsistentsRead){
        this.first = first;
        this.second = second;
        this.totalSegments = totalSegments;
        this.segmentNum = segmentNum;
        this.itemLimit = itemLimit;
        this.isConsistentRead = isConsistentsRead;
        this.numCompares = new AtomicLong();
        this.equals = new AtomicLong();
        this.notEquals = new AtomicLong();
    }

    @Override
    public TableCompareResult call() {
        LOGGER.info("Running with data segment num " + segmentNum + " totalDone " + numCompares);
        Map<String, AttributeValue> exclusiveStartKey1 ;
        Map<String, AttributeValue> exclusiveStartKey2 ;
        RateLimiter limiter = RateLimiter.create(itemLimit);
        ExecutorService parallelSegmentService = Executors.newFixedThreadPool(2);
        ScanResultDecorator firstRequest = new ScanResultDecorator(first.getLeft(), first.getRight(), null, this.isConsistentRead, this.itemLimit, this.totalSegments, this.segmentNum);
        ScanResultDecorator secondRequest = new ScanResultDecorator(second.getLeft(),second.getRight(), null, this.isConsistentRead, this.itemLimit, this.totalSegments, this.segmentNum);
        try {
            while (true) {
                limiter.acquire(itemLimit);
                ScanResult firstResult = parallelSegmentService.submit(firstRequest).get();
                ScanResult secondResult = parallelSegmentService.submit(secondRequest).get();

                if (firstResult == null || secondResult == null){
                    throw new Exception("Unable to get results from future");
                }

                Collection<Map<String, AttributeValue>> item1 = firstResult.getItems();
                Collection<Map<String, AttributeValue>> item2 = secondResult.getItems();

                if (item1 != null && item2 != null) {
                    LOGGER.info("Retrieved  items for segment  : " + segmentNum + " totalCompared " + numCompares + "equalCount : " + equals + "notEqual:" + notEquals);
                    Iterator<Map<String, AttributeValue>> item1Iter = item1.iterator();
                    Iterator<Map<String, AttributeValue>> item2Iter = item2.iterator();
                    while (item1Iter.hasNext() && item2Iter.hasNext()) {
                        numCompares.addAndGet(1);
                        if (compareItems(item1Iter.next(), item2Iter.next())) {
                            equals.getAndAdd(1);
                           // LOGGER.info("Eq"+equals.longValue());

                        } else {
                            notEquals.getAndAdd(1);
                            LOGGER.error("Retrieved items that are not equal in parallel scan: this should not be possible if table matches ");
                            break;
                        }

                    }
                }

                exclusiveStartKey1 = firstResult.getLastEvaluatedKey();
                exclusiveStartKey2 = secondResult.getLastEvaluatedKey();

                if (exclusiveStartKey1 == null && exclusiveStartKey2 == null) {

                    LOGGER.info("Completed eval matches" + equals + " not equal" + notEquals);
                    break;
                }
                else {
                    LOGGER.debug("Setting exclusive start key for first table to " + exclusiveStartKey1);
                    LOGGER.debug("Setting exclusive start key for second table to " + exclusiveStartKey2);
                    firstRequest.setExclusiveStartKey(exclusiveStartKey1);
                    secondRequest.setExclusiveStartKey(exclusiveStartKey2);
                }

            }
        } catch (Exception e) {
            LOGGER.error("Error", e);
        }
        LOGGER.info("done task with thread equals : " + equals + " not equals " + notEquals);
        TableCompareResult result =  new TableCompareResult();

        result.numCompares = numCompares.longValue();
        result.equals = equals.longValue();
        result.notEquals = notEquals.longValue();

        return result;
    }

    private static <K, V> boolean compareItems(Map<K, V> one, Map<K, V> two) {
        if (one == two) return true;
        if (one.size() != two.size()) {
            LOGGER.error("no size match" + one + "two" + two);
            return false;
        }
        LOGGER.debug("First Key" + one.toString() + " Second key" + two.toString());

        for (K key : one.keySet()) {
            if (one.get(key) != null) {
                if (!one.get(key).equals(two.get(key))) {
                    LOGGER.error("no key match" +key + "value: first " + one.get(key) + "second" + two.get(key));
                    return false;
                }
            }
        }

        return true;
    }


    public static class ScanResultDecorator implements Callable<ScanResult> {
        private final String tableName;
        private final int totalSegments;
        private final int segmentNum;
        private final AmazonDynamoDBClient amazonDynamoDBClient;
        private final Boolean isConsistentRead;
        private  volatile Map<String,AttributeValue> exclusiveStartKey;
        private final int limit;
        public ScanResultDecorator(AmazonDynamoDBClient amazonDynamoDBClient, String tableName, Map<String,AttributeValue> exclusiveStartKey, Boolean isConsistentRead,  int limit, int totalSegments, int segmentNum){
            this.amazonDynamoDBClient = amazonDynamoDBClient;
            this.tableName = tableName;
            this.exclusiveStartKey = exclusiveStartKey;
            this.limit = limit;
            this.isConsistentRead = isConsistentRead;
            this.totalSegments = totalSegments;
            this.segmentNum = segmentNum;
        }

        public void setExclusiveStartKey(Map<String,AttributeValue> exclusiveStartKey){
            this.exclusiveStartKey = exclusiveStartKey;
        }

        @Override
        public ScanResult call() throws Exception {

            return amazonDynamoDBClient.scan(new ScanRequest()
                    .withTableName(this.tableName)
                    .withLimit(this.limit)
                    .withConsistentRead(this.isConsistentRead)
                    .withExclusiveStartKey(this.exclusiveStartKey)
                    .withTotalSegments(this.totalSegments)
                    .withSegment(this.segmentNum));
        }
    }
}

