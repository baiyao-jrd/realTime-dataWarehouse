package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Felix
 * @date 2022/9/6
 * 流量域：用户跳出明细事实表
 * 需要启动的进程
 *      zk、kafka、flume、DwdTrafficBaseLogSplit、DwdTrafficUserJumpDetail
 */
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的page_log主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

       /* // 测试数据
        DataStream<String> kafkaStrDS = env
            .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"home\"},\"ts\":55000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                    "\"detail\"},\"ts\":15000} "
            );*/


        //TODO 4.对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // jsonObjDS.print(">>>>");


        //TODO 5.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
            WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
            // WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                            return jsonObj.getLong("ts");
                        }
                    }
                )
        );

        //TODO 6.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 7.使用Flink的CEP判断是否为跳出行为
        //7.1 定义pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
            new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) {
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    return StringUtils.isEmpty(lastPageId);
                }
            }
        ).next("second").where(
            new SimpleCondition<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) {
                    String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                    return StringUtils.isEmpty(lastPageId);
                }
            }
        ).within(Time.seconds(10));
        //7.2 将pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedDS, pattern);

        //7.3 从流中提取数据(完全匹配 +  超时数据)
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOutTag") {};
        SingleOutputStreamOperator<String> matchDS = patternDS.select(
            timeOutTag,
            new PatternTimeoutFunction<JSONObject, String>() {
                @Override
                public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                    //处理超时数据  注意：如果是超时处理，方法的返回值会放到侧输出流中
                    return pattern.get("first").get(0).toJSONString();
                }
            },
            new PatternSelectFunction<JSONObject, String>() {
                @Override
                public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                    //处理完全匹配的数据   注意：方法的返回值输出到主流中
                    return pattern.get("first").get(0).toJSONString();
                }
            }
        );

        //TODO 8.将完全匹配的数据和超时数据进行合并
        DataStream<String> ujdDS = matchDS.union(matchDS.getSideOutput(timeOutTag));

        //TODO 9.将跳出行为发送kafka主题中
        ujdDS.print(">>>");
        ujdDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));

        env.execute();
    }
}