package com.atguigu.gmall.realtime.util;

/**
 * @author Felix
 * @date 2022/9/8
 * 操作MySQL的工具类
 */
public class MySqlUtil {
    //获取从mysql中读取字典表数据创建FlinkSQL的动态表DDL
    public static String getBaseDicLookUpDDL() {
        return "CREATE TABLE base_dic (\n" +
            "  dic_code STRING,\n" +
            "  dic_name STRING,\n" +
            "  PRIMARY KEY (dic_code) NOT ENFORCED\n" +
            ") " + getJdbcDDL("base_dic");
    }

    //获取jdbc连接器相关连接属性
    public static String getJdbcDDL(String tableName) {
        return " WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            "   'url' = 'jdbc:mysql://hadoop202:3306/gmall0321',\n" +
            "   'table-name' = '" + tableName + "',\n" +
            "   'lookup.cache.max-rows' = '200',\n" +
            "   'lookup.cache.ttl' = '1 hour',\n" +
            "   'username' = 'root',\n" +
            "   'password' = '123456'\n" +
            ")";
    }
}
