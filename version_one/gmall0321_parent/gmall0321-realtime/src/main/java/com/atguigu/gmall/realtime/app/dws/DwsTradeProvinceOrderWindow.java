package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2022/9/17
 * 交易域：省份粒度下单业务过程聚合统计
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis、clickhouse
 *      DwdTradeOrderPreProcess、DwdTradeOrderDetail、DwsTradeProvinceOrderWindow
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/xxx");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 3.从kafka主题中读取下单事实表数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        //TODO 4.对流中数据类型进行转换        jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        // {"create_time":"2022-09-01 10:15:55","sku_num":"1","activity_rule_id":"3",
        // "split_original_amount":"8197.0000","sku_id":"11","date_id":"2022-09-01",
        // "source_type_name":"用户查询","user_id":"150","province_id":"20","source_type_code":"2401",
        // "row_op_ts":"2022-09-17 02:15:56.439Z","activity_id":"2","sku_name":"Apple iPhone","id":"1182",
        // "order_id":"514","split_activity_amount":"200.0","split_total_amount":"7997.0","ts":"1663380955"}
        // jsonObjDS.print(">>>>");
        //TODO 5.按照订单明细id进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 6.使用Flink的状态编程 + 定时器进行去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> lastValueState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastValueState
                        = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("lastValueState", JSONObject.class));
                }

                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject lastJsonObj = lastValueState.value();
                    if (lastJsonObj == null) {
                        lastValueState.update(jsonObj);
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                    } else {
                        String lastRowOpTs = lastJsonObj.getString("row_op_ts");
                        String curRowOpTs = jsonObj.getString("row_op_ts");
                        if (TimestampLtz3CompareUtil.compare(lastRowOpTs, curRowOpTs) <= 0) {
                            lastValueState.update(jsonObj);
                        }
                    }

                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject jsonObj = lastValueState.value();
                    if (jsonObj != null) {
                        out.collect(jsonObj);
                    }
                    lastValueState.clear();
                }
            }
        );
        //TODO 7.对流中数据类型进行转换        jsonObj->实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> orderBeanDS = distinctDS.map(
            new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                @Override
                public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                    String provinceId = jsonObj.getString("province_id");
                    Long ts = jsonObj.getLong("ts") * 1000;
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    String orderId = jsonObj.getString("order_id");

                    TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                        .provinceId(provinceId)
                        .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                    return orderBean;
                }
            }
        );
        //TODO 8.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                        @Override
                        public long extractTimestamp(TradeProvinceOrderBean orderBean, long recordTimestamp) {
                            return orderBean.getTs();
                        }
                    }
                )
        );
        //TODO 9.按照省份进行分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = withWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 10.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 11.聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
            new ReduceFunction<TradeProvinceOrderBean>() {
                @Override
                public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                    return value1;
                }
            },
            new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                    String start = DateFormatUtil.toYmdHms(window.getStart());
                    String end = DateFormatUtil.toYmdHms(window.getEnd());
                    for (TradeProvinceOrderBean orderBean : input) {
                        orderBean.setStt(start);
                        orderBean.setEdt(end);
                        orderBean.setTs(System.currentTimeMillis());
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
            }
        );
        // reduceDS.print(">>>>");
        //TODO 12.和省份的维度进行关联
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = AsyncDataStream.unorderedWait(
            reduceDS,
            new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province") {
                @Override
                public void join(TradeProvinceOrderBean orderBean, JSONObject dimInfoJsonObj) {
                    orderBean.setProvinceName(dimInfoJsonObj.getString("NAME"));
                }

                @Override
                public String getKey(TradeProvinceOrderBean orderBean) {
                    return orderBean.getProvinceId();
                }
            },
            60, TimeUnit.SECONDS
        );

        //TODO 13.将关联结果写到Clickhouse表中
        withProvinceDS.print(">>>>>");
        withProvinceDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));
        env.execute();
    }
}
