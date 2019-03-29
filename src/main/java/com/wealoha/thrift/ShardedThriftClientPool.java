package com.wealoha.thrift;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wealoha.thrift.exception.NoBackendServiceException;

/**
 * A sharded wrapper for {@link ThriftClientPool}<br/>
 * 
 * List<ServiceInfo> -> List<List<ServiceInfo>><br/>
 * for example: three node 10.0.1.1:9000,10.0.1.2:9000,10.0.1.3:9000, <br/>
 * will divide into 3 partitions: list(10.0.1.1:9000),
 * list(10.0.1.2:9000),
 * list(10.0.1.3:9000)<br/>
 * 
 * if a key's hash is 10, then 10 % 3 = 1, selected node will be
 * 10.0.1.2:9000
 * 
 * @author javamonk
 * @createTime 2015年6月5日 上午11:58:08
 */
public class ShardedThriftClientPool<K, T extends TServiceClient> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private List<ServiceInfo> serviceList;

    private final Function<K, Long> hashFunction;

    private final Function<List<ServiceInfo>, List<List<ServiceInfo>>> partitionFunction;

    private final Function<List<ServiceInfo>, ThriftClientPool<T>> clientPoolFunction;

    private Map<Integer, ThriftClientPool<T>> poolMap;

    private List<List<ServiceInfo>> servicePartitions;

    /**
     * @param serviceList
     * @param hashFunction get a key's hash
     * @param partitionFunction split list of {@link ServiceInfo} to
     *        partition
     * 
     * @param clientPoolFunction
     */
    public ShardedThriftClientPool(List<ServiceInfo> serviceList,
            Function<K, Long> hashFunction,
            Function<List<ServiceInfo>, List<List<ServiceInfo>>> partitionFunction,
            Function<List<ServiceInfo>, ThriftClientPool<T>> clientPoolFunction) {

        this.hashFunction = hashFunction;
        this.partitionFunction = partitionFunction;
        this.clientPoolFunction = clientPoolFunction;

        init(serviceList);
    }

    /**
     * one server one partition
     * 
     * @param serviceList
     * @param hashFunction get a key's hash
     * 
     * @param clientPoolFunction
     */
    public ShardedThriftClientPool(List<ServiceInfo> serviceList,
            Function<K, Long> hashFunction,
            Function<List<ServiceInfo>, ThriftClientPool<T>> clientPoolFunction) {
        this(serviceList, hashFunction, servers -> servers.stream()
                .map(server -> Collections.singletonList(server)).collect(Collectors.toList()),
                clientPoolFunction);
    }

    private void init(List<ServiceInfo> services) {
        if (services == null || services.size() == 0) {
            throw new IllegalArgumentException("serviceList is empty");
        }
        poolMap = new HashMap<>();
        this.serviceList = services;
        servicePartitions = partitionFunction.apply(serviceList);
        if (servicePartitions == null || servicePartitions.size() == 0) {
            throw new IllegalStateException("partitionFunction should not return empty");
        }
    }

    static BigDecimal MAX_HASH = new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(Long.MAX_VALUE)).add(new BigDecimal(1));
    static long MAX_HASH2 = -1;

    public static int findServerPartitionIndex(BigDecimal hash, int bucketSize) {

        BigDecimal divideSize = MAX_HASH.divide(new BigDecimal(bucketSize), BigDecimal.ROUND_DOWN);
        if (hash.max(divideSize.multiply(new BigDecimal(bucketSize))).equals(hash)) {
            return bucketSize;
        } else {
            return hash.divide(divideSize, BigDecimal.ROUND_DOWN).intValue() + 1;
        }
    }

    public static int findServerPartitionIndex2(long hash, int bucketSize) {

        long divideSize = Long.divideUnsigned(MAX_HASH2, bucketSize);
        long upBound = divideSize * bucketSize;
        if (Long.compareUnsigned(hash, upBound) >= 0L) {
            return bucketSize;
        } else {
            return (int) Long.divideUnsigned(hash, divideSize) + 1;
        }
    }


    public static void main(String[] args) {
        /*System.out.println(BigDecimal.valueOf(10).max(BigDecimal.valueOf(12)));
        BigDecimal b1 = BigDecimal.valueOf(10);
        BigDecimal b2 = BigDecimal.valueOf(12);
        System.out.println(b1.min(b2).equals(b1));*/

        System.out.println(findServerPartitionIndex(MAX_HASH, 2048));
        System.out.println(findServerPartitionIndex2(-1, 2048));

        System.out.println(findServerPartitionIndex(BigDecimal.valueOf(100).add(BigDecimal.valueOf(Long.MAX_VALUE)), 2048));
        System.out.println(findServerPartitionIndex2(100 + Long.MAX_VALUE, 2048));
    }


    public ThriftClientPool<T> getShardedPool(K key) throws NoBackendServiceException {
        long hash = hashFunction.apply(key);
        // int shard = hash % servicePartitions.size();
        // int shard = findServerPartitionIndex(new BigDecimal(hash), servicePartitions.size());
        int shard = findServerPartitionIndex2(hash, servicePartitions.size());
        logger.debug("getPool by key: hash={}, shard={}/{}", hash, shard, servicePartitions.size());

        List<ServiceInfo> servers = servicePartitions.get(shard - 1);
        if (servers == null || servers.size() == 0) {
            throw new NoBackendServiceException("no servers mapping for key: " + key);
        }

        ThriftClientPool<T> pool = poolMap.get(shard);
        if (pool == null) {
            synchronized (poolMap) {
                pool = poolMap.get(shard);
                if (pool == null) {
                    logger.debug("init client pool: shard={}, servers={}", shard, servers);
                    pool = clientPoolFunction.apply(servers);
                    poolMap.put(shard, pool);
                }
            }
        }
        return pool;
    }

    /**
     * set new services for this pool(TODO may throw
     * {@link IndexOutOfBoundsException} at {@link #getShardedPool(Object)}
     * in heavy load environment)
     *
     * @param services
     * @return previous pool, you must release each pool(for example wait
     *         some seconds while no access to pool)
     */
    public Map<Integer, ThriftClientPool<T>> setServices(List<ServiceInfo> services) {
        synchronized (poolMap) {
            logger.info("reinit pool using new serviceList: {}", services);
            Map<Integer, ThriftClientPool<T>> previousMap = poolMap;

            init(services);
            return previousMap;
        }
    }

    public int getPartitionSize() {
        return servicePartitions.size();
    }
}
