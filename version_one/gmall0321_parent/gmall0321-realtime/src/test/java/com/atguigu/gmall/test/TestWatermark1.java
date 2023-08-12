package com.atguigu.gmall.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Felix
 * @date 2022/8/19
 * 该案例演示了Watermark的使用
 */
public class TestWatermark1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env
            .socketTextStream("hadoop202", 8888)
            .map(new MapFunction<String, Emp>() {
                @Override
                public Emp map(String str) throws Exception {
                    String[] fieldArr = str.split(",");
                    return new Emp(
                        Integer.parseInt(fieldArr[0]),
                        fieldArr[1],
                        Integer.parseInt(fieldArr[2]),
                        Long.valueOf(fieldArr[3])
                    );
                }
            }).assignTimestampsAndWatermarks(
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
        )
            .keyBy(Emp::getEmpno)
            .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            .process(new ProcessWindowFunction<Emp, Emp, Integer, TimeWindow>() {
                         @Override
                         public void process(Integer integer, Context context, Iterable<Emp> elements, Collector<Emp> out) throws Exception {
                             System.out.println(context.currentWatermark());
                             for (Emp emp : elements) {
                                 out.collect(emp);
                             }
                         }
                     }
            ).print(">>>>");

        env.execute();
    }
}
