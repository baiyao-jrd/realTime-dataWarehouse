package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/9/7
 * 该案例演示了FlinkSQL的join
 *                                  左表                    右表
 *      内连接                  OnCreateAndWrite        OnCreateAndWrite
 *      左外连接                OnReadAndWrite          OnCreateAndWrite
 *      右外连接                OnCreateAndWrite        OnReadAndWrite
 *      全外连接                OnReadAndWrite          OnReadAndWrite
 */
public class TestSqlJoin {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //1.4 设置状态的TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //TODO 2.准备关联的流
        SingleOutputStreamOperator<Emp> empDS =
            env
                .socketTextStream("hadoop202", 8888)
                .map(
                    strLine -> {
                        String[] fieldArr = strLine.split(",");
                        Emp emp = new Emp(Integer.parseInt(fieldArr[0]), fieldArr[1],
                            Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                        return emp;
                    }
                );
        SingleOutputStreamOperator<Dept> deptDS =
            env
                .socketTextStream("hadoop202", 8889)
                .map(
                    lineStr -> {
                        String[] fieldArr = lineStr.split(",");
                        return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                    }
                );

        //TODO 3.将流转换为临时表
        tableEnv.createTemporaryView("emp", empDS);
        tableEnv.createTemporaryView("dept", deptDS);

        //TODO 4.内连接
        // 注意：如果使用FlinkSQL的普通内外连接的话，底层会维护两个状态，用于存放左表和右表的数据
        // 默认情况下，状态永不失效；在生产环境，需要设置状态的TTL
        // tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e " +
        //     "join dept d on e.deptno = d.deptno").print();

        //TODO 5.左外连接
        //左外连接的连接过程
        //如果左表数据先来，右表数据后到，首先会先生成一条数据 [左 null]，标记为+I；
        //然后右表数据来到的时候，会生成一条数据[左 null]，标记为-D
        //最后生成一条数据[左  右] ，标记为+I。这种动态表转换的流称之为回撤流
        // tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e " +
        //     "left join dept d on e.deptno = d.deptno").print();

        //TODO 6.右外连接
        // tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e " +
        //     " right join dept d on e.deptno = d.deptno").print();

        //TODO 7.全外连接
        // tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e " +
        //     " full join dept d on e.deptno = d.deptno").print();

        //TODO 8.将左外连接的结果写到kafka的主题中
        Table empTable = tableEnv.sqlQuery("select e.empno,e.ename,d.deptno,d.dname from emp e " +
            "left join dept d on e.deptno = d.deptno");
        tableEnv.createTemporaryView("emp_table",empTable);

        tableEnv.executeSql("CREATE TABLE emp_k (\n" +
            "  empno INTEGER,\n" +
            "  ename STRING,\n" +
            "  deptno integer,\n" +
            "  dname string,\n" +
            "  PRIMARY KEY (empno) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'upsert-kafka',\n" +
            "  'topic' = 'first',\n" +
            "  'properties.bootstrap.servers' = 'hadoop202:9092',\n" +
            "  'key.format' = 'json',\n" +
            "  'value.format' = 'json'\n" +
            ")");

        tableEnv.executeSql("insert into emp_k select * from emp_table");

    }
}
