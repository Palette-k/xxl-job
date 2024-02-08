package com.xxl.job.admin.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistencyHashUtil {

    /**
     * 实际节点
     */
    private List<String> shardNodes;

    /**
     * 存储节点数
     */
    private final int NODE_NUM = 1000;

    /**
     * 映射到哈希环上的 虚拟节点+真实节点 (使用 红黑树 排序)
     */
    private TreeMap<Long, String> virtualHash2RealNode = new TreeMap<Long, String>();
    /**
     * 初始化节点（引入虚拟节点）
     * init consistency hash ring, put virtual node on the 2^64 ring
     */
    public void initVirtual2RealRing(List<String> shards) {
        this.shardNodes = shards;
        for (String node : shardNodes) {
            for (int i = 0; i < NODE_NUM; i++){
                long hashCode = hash("SHARD-" + node + "-NODE-" + i);
                virtualHash2RealNode.put(hashCode, node);
            }
        }
    }
    /**
     * 寻找数据所对应节点
     * 从顺时针遇到的第一个节点
     * get real node by key's hash on the 2^64
     */
    public String getShardInfo(String key) {
        long hashCode = hash(key);
        SortedMap<Long, String> tailMap = virtualHash2RealNode.tailMap(hashCode);
        if (tailMap.isEmpty()) {
            return virtualHash2RealNode.get(virtualHash2RealNode.firstKey());
        }
        return virtualHash2RealNode.get(tailMap.firstKey());
    }
    /**
     * 打印节点
     * prinf ring virtual node info
     */
    public void printMap() {
        System.out.println(virtualHash2RealNode);
    }
    /**
     *  MurMurHash算法，是非加密HASH算法，性能很高，
     *  比传统的CRC32,MD5，SHA-1（这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免）
     *  等HASH算法要快很多，而且据说这个算法的碰撞率很低.
     *  http://murmurhash.googlepages.com/
     */
    public static Long hash(String key) {
        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;
        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        long m = 0xc6a4a7935bd1e995L;
        int r = 47;
        long h = seed ^ (buf.remaining() * m);
        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
        }
        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(
                    ByteOrder.LITTLE_ENDIAN);
            // for big-endian version, do this first:
            // finish.position(8-buf.remaining());
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        buf.order(byteOrder);
        return h;
    }
    /**
     * get hash code on 2^32 ring (md5散列的方式计算hash值)
     * @param key
     * @return long
     */
    public static long hash2(String key) {
        // md5 byte
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        byte[] keyBytes = null;
        try {
            keyBytes = key.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unknown string :" + key, e);
        }
        md5.update(keyBytes);
        byte[] digest = md5.digest();
        // hash code, Truncate to 32-bits
        long hashCode = ((long) (digest[3] & 0xFF) << 24)
                | ((long) (digest[2] & 0xFF) << 16)
                | ((long) (digest[1] & 0xFF) << 8)
                | (digest[0] & 0xFF);
        long truncateHashCode = hashCode & 0xffffffffL;
        return truncateHashCode;
    }
    public static void main(String[] args) {
        List<String> shards = new ArrayList<String>();
        shards.add("consumer-uuid-2");
        shards.add("consumer-uuid-1");
        ConsistencyHashUtil sh = new ConsistencyHashUtil();
        sh.initVirtual2RealRing(shards);
        sh.printMap();
        int consumer1 = 0;
        int consumer2 = 0;
        for (int i = 0; i < 10000; i++) {
            String key = "consumer" + i;
            System.out.println(hash(key) + ":" + sh.getShardInfo(key));
            if ("consumer-uuid-1".equals(sh.getShardInfo(key))) {
                consumer1++;
            }
            if ("consumer-uuid-2".equals(sh.getShardInfo(key))) {
                consumer2++;
            }
        }
        System.out.println("consumer1:" + consumer1);
        System.out.println("consumer2:" + consumer2);
        /*long start = System.currentTimeMillis();
        for (int i = 0; i < 1000 * 1000 * 1000; i++) {
            if (i % (100 * 1000 * 1000) == 0) {
                System.out.println(i + ":" + hash("key1" + i));
            }
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);*/
    }
}
