package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Felix
 * @date 2022/9/15
 * 查询维度的工具类
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection conn, String tableName, String id) {
        return getDimInfo(conn, tableName, Tuple2.of("id", id));
    }

    /**
     * @param conn
     * @param tableName
     * @param columnNameAndValues
     * @return 获取维度对象
     * 优化：旁路缓存
     * 思路：先从缓存中获取维度数据，如果缓存中存在维度数据，直接返回(缓存命中)；如果缓存中不存在维度数据，
     * 再发送请求到phoenix表中查询维度数据，并将查询出来的维度数据放到缓存中缓存起来，方便下次使用。
     * 选型：
     * Redis √
     * 性能也不错、维护性好
     * 状态
     * 性能好、维护性差
     * Redis使用分析：
     * 类型：     String
     * key：     dim:维度表表名:主键1_主键2
     * TTL:      1day
     * 注意： 如果业务数据库维度表发生了变化，将缓存中的维度删除
     */
    public static JSONObject getDimInfo(Connection conn, String tableName, Tuple2<String, String>... columnNameAndValues) {
        //拼接从Redis中查询维度的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            redisKey.append(columnValue);
            selectSql.append(columnName + "='" + columnValue + "'");
            if (i < columnNameAndValues.length - 1) {
                redisKey.append("_");
                selectSql.append(" and ");
            }
        }

        Jedis jedis = null;
        String dimInfoStr = null;
        JSONObject dimInfoJsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            dimInfoStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("~~从Redis中查询维度数据发生了异常~~");
        }

        if (StringUtils.isNotEmpty(dimInfoStr)) {
            //说明从redis中找到了对应的维度数据(缓存命中)
            dimInfoJsonObj = JSON.parseObject(dimInfoStr);
        } else {
            //说明从redis中没有找到对应的维度数据,发送请求到phoenix表中查询维度
            System.out.println("从phoenix表中查询维度的SQL:" + selectSql);
            //查询维度的时候，底层还是调用的是PhoenixUtil中的queryList
            List<JSONObject> dimInfoJsonList = PhoenixUtil.queryList(conn, selectSql.toString(), JSONObject.class);
            if (dimInfoJsonList != null && dimInfoJsonList.size() > 0) {
                //注意:因为我们是根据维度的主键的去查询维度数据，所以如果集合不为空，那么返回的维度只会有一条
                dimInfoJsonObj = dimInfoJsonList.get(0);
                //将查询的结果放到redis中进行缓存,并指定失效时间
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 3600 * 24, dimInfoJsonObj.toJSONString());
                }
            } else {
                System.out.println("~~在phoenix表中没有查到对应的维度信息~~");
            }
        }

        //关闭连接
        if (jedis != null) {
            System.out.println("~~关闭Jedis客户端~~");
            jedis.close();
        }

        return dimInfoJsonObj;
    }

    //查询维度数据 没有任何优化
    public static JSONObject getDimInfoNoCache(Connection conn, String tableName, Tuple2<String, String>... columnNameAndValues) {

        StringBuilder selectSql = new StringBuilder("select * from " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + " where ");
        for (int i = 0; i < columnNameAndValues.length; i++) {
            Tuple2<String, String> columnNameAndValue = columnNameAndValues[i];
            String columnName = columnNameAndValue.f0;
            String columnValue = columnNameAndValue.f1;
            selectSql.append(columnName + "='" + columnValue + "'");
            if (i < columnNameAndValues.length - 1) {
                selectSql.append(" and ");
            }
        }
        System.out.println("从phoenix表中查询维度的SQL:" + selectSql);
        //查询维度的时候，底层还是调用的是PhoenixUtil中的queryList
        List<JSONObject> dimInfoJsonList = PhoenixUtil.queryList(conn, selectSql.toString(), JSONObject.class);
        JSONObject dimInfoJsonObj = null;
        if (dimInfoJsonList != null && dimInfoJsonList.size() > 0) {
            //注意:因为我们是根据维度的主键的去查询维度数据，所以如果集合不为空，那么返回的维度只会有一条
            dimInfoJsonObj = dimInfoJsonList.get(0);
        } else {
            System.out.println("~~在phoenix表中没有查到对应的维度信息~~");
        }
        return dimInfoJsonObj;
    }


    public static void delCached(String tableName, String id) {
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
        //dim:维度表表名:主键1_主键2
        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(jedis != null){
                System.out.println("~~从Redis中删除维度后，关闭Jedis客户端~~");
                jedis.close();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection conn = dataSource.getConnection();
        // System.out.println(getDimInfoNoCache(conn, "DIM_BASE_TRADEMARK", Tuple2.of("id","1")));
        System.out.println(getDimInfo(conn, "DIM_BASE_TRADEMARK", "1"));
    }
}
