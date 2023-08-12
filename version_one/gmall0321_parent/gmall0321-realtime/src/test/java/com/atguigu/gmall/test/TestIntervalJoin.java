package com.atguigu.gmall.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2022/9/7
 * 该案例演示了intervalJoin的基本用法
 */
public class TestIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //从nc中读取员工信息     10  zs  100  9
        SingleOutputStreamOperator<Emp> empDS = env
            .socketTextStream("hadoop202", 8888)
            .map(
                strLine -> {
                    String[] fieldArr = strLine.split(",");
                    Emp emp = new Emp(Integer.parseInt(fieldArr[0]), fieldArr[1],
                        Integer.parseInt(fieldArr[2]), Long.parseLong(fieldArr[3]));
                    return emp;
                }
            ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Emp>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Emp>() {
                            @Override
                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                return emp.getTs();
                            }
                        }
                    )
            );

        empDS.print("emp:");
        SingleOutputStreamOperator<Dept> deptDS =
            env.socketTextStream("hadoop202", 8889).map(
                lineStr -> {
                    String[] fieldArr = lineStr.split(",");
                    return new Dept(Integer.parseInt(fieldArr[0]), fieldArr[1], Long.parseLong(fieldArr[2]));
                }
            ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Dept>forMonotonousTimestamps()
                    .withTimestampAssigner(
                        new SerializableTimestampAssigner<Dept>() {
                            @Override
                            public long extractTimestamp(Dept dept, long recordTimestamp) {
                                return dept.getTs();
                            }
                        }
                    )
            );

        deptDS.print("dept");

        //使用intervalJoin对两条流进行关联
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> processDS = empDS
            .keyBy(Emp::getDeptno)
            .intervalJoin(deptDS.keyBy(Dept::getDeptno))
            .between(Time.milliseconds(-5), Time.milliseconds(5))
            .process(
                new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp left, Dept right, Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                }
            );

        processDS.print(">>>>");
        env.execute();
    }
}
