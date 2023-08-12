package com.atguigu.gmall.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Felix
 * @date 2022/9/16
 * 操作Redis的工具类
 */
public class RedisUtil {
    private static JedisPool jedisPool;
    static{
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMinIdle(5);
        poolConfig.setMaxIdle(5);
        poolConfig.setMaxTotal(100);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000L);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig,"hadoop202",6379,10000);
    }
    //获取Jedis客户端
    public static Jedis getJedis(){
        System.out.println("~~获取Jedis客户端~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }
}
