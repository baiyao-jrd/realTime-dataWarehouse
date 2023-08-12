package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2022/9/7
 * 该案例演示了lookup join
 *  FlinkAPI的join方式
 *      -基于窗口
 *          滚动窗口
 *          滑动窗口
 *          会话窗口
 *      -基于状态
 *          intervalJoin
 *              底层实现：connect  + 状态
 *              执行流程: 是否迟到、添加到状态、和另一条流缓存的数据进行关联、清除状态
 *      -不足：不支持外连接
 *          可以通过connect、congroup手动自己实现
 * FlinkSQL
 *      内连接、外连接
 *      对于普通的内外连接，在底层维护了两个状态，默认情况下，这两个状态永不失效的
 *      在生产环境中，可以通过tableEnv.getConfig().setIdleStateRetention();设置状态的TTL
 *                                    左表                    右表
 *        内连接                  OnCreateAndWrite        OnCreateAndWrite
 *        左外连接                OnReadAndWrite          OnCreateAndWrite
 *        右外连接                OnCreateAndWrite        OnReadAndWrite
 *        全外连接                OnReadAndWrite          OnReadAndWrite
 *      左外连接的连接过程：如果左表数据先来，右表数据后到，首先会先生成一条数据 [左 null]，标记为+I；
 *       然后右表数据来到的时候，会生成一条数据[左 null]，标记为-D，最后生成一条数据[左  右] ，标记为+I。这种动态表转换的流称之为回撤流
 *      ----------
 *      loopUp join
 *          lookup join的底层实现过程和普通的内外连接完全不一样。
 *          lookup join底层没有维护两个状态，用于存放连接的两张表的数据。它是以左表进行驱动，也就是说，
 *         每当有一条左表数据进来的时候，会发送请求到外部系统，和外部系统的表数据进行连接查询。
 *      设置缓存参数据
 *             'lookup.cache.max-rows'
 *             'lookup.cache.ttl' = '1 hour'
 *  连接器
 *      kafka
 *      upsert-kafka
 *      jdbc
 */
public class TestLookupJoin {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 2.从kafka主题中获取员工表数据
        tableEnv.executeSql("CREATE TABLE emp (\n" +
            "  empno integer,\n" +
            "  ename string,\n" +
            "  deptno integer,\n" +
            "  proc_time AS PROCTIME()\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'first',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'properties.group.id' = 'testGroup',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'format' = 'json'\n" +
            ")");
        // tableEnv.executeSql("select * from emp").print();


        //TODO 3.从MySQL数据库表中获取部门数据
        tableEnv.executeSql("CREATE TABLE dept (\n" +
            "  deptno integer,\n" +
            "  dname STRING,\n" +
            "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
            ") WITH (\n" +
            "   'connector' = 'jdbc',\n" +
            "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            "   'url' = 'jdbc:mysql://hadoop202:3306/gmall0321_config',\n" +
            "   'lookup.cache.max-rows' = '200',\n" +
            "   'lookup.cache.ttl' = '1 hour',\n" +
            "   'table-name' = 't_dept',\n" +
            "   'username' = 'root',\n" +
            "   'password' = '123456'\n" +
            ")");
        // tableEnv.executeSql("select * from dept").print();

        //TODO 4.使用lookup join 对两张表进行关联
        //注意：lookup join的底层实现过程和普通的内外连接完全不一样。
        //lookup join底层没有维护两个状态，用于存放连接的两张表的数据。它是以左表进行驱动，也就是说，
        //每当有一条左表数据进来的时候，会发送请求到外部系统，和外部系统的表数据进行连接查询。
        Table resTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname FROM emp AS e JOIN dept \n" +
            " FOR SYSTEM_TIME AS OF e.proc_time AS d ON e.deptno = d.deptno");
        tableEnv.createTemporaryView("res_table",resTable);
        // tableEnv.executeSql("select * from res_table").print();

        //TODO 5.将关联的结果写到kafka主题中
        tableEnv.executeSql(" CREATE TABLE emp_k (\n" +
            "  empno integer,\n" +
            "  ename STRING,\n" +
            "  deptno integer,\n" +
            "  dname string,\n" +
            "  PRIMARY KEY (empno) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = 'second',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")");

        tableEnv.executeSql("insert into emp_k select * from res_table");

    }
}
