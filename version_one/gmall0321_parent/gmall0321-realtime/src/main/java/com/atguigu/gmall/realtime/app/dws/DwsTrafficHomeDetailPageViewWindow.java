package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/9/13
 * 流量域：首页、详情页聚合统计(独立访客)
 * 需要启动的进程
 * zk、kafka、flume、DwdTrafficBaseLogSplit、DwsTrafficHomeDetailPageViewWindow
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的page_log主题中读取数据
        //3.1 声明消费主题以及消费者组
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_home_detail_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        //TODO 4.对读取的数据进行类型转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //TODO 5.过滤出首页以及详情页的日志访问
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
            new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {
                    String pageId = jsonObj.getJSONObject("page").getString("page_id");
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        return true;
                    }
                    return false;
                }
            }
        );
        // filterDS.print(">>>>>");
        //TODO 6.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<JSONObject>() {
                        @Override
                        public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                            return jsonObj.getLong("ts");
                        }
                    }
                )
        );
        //TODO 7.按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //TODO 8.使用Flink的状态编程  判断是否是首页以及详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processDS = keyedDS.process(
            new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                private ValueState<String> homeLastVisitDateState;
                private ValueState<String> detailLastVisitDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> homeValueStateDescriptor
                        = new ValueStateDescriptor<String>("homeLastVisitDateState", String.class);
                    homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                    ValueStateDescriptor<String> detailValueStateDescriptor
                        = new ValueStateDescriptor<String>("detailLastVisitDateState", String.class);
                    detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);
                }

                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    String pageId = jsonObj.getJSONObject("page").getString("page_id");

                    Long ts = jsonObj.getLong("ts");
                    String curVisitDate = DateFormatUtil.toDate(ts);
                    Long homeUvCt = 0L;
                    Long detailUvCt = 0L;
                    if ("home".equals(pageId)) {
                        String homeLastVisitDate = homeLastVisitDateState.value();
                        if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                            homeUvCt = 1L;
                            homeLastVisitDateState.update(curVisitDate);
                        }
                    }

                    if ("good_detail".equals(pageId)) {
                        String detailLastVisitDate = detailLastVisitDateState.value();
                        if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                            detailUvCt = 1L;
                            detailLastVisitDateState.update(curVisitDate);
                        }
                    }

                    if (homeUvCt != 0L || detailUvCt != 0L) {
                        out.collect(new TrafficHomeDetailPageViewBean(
                            "",
                            "",
                            homeUvCt,
                            detailUvCt,
                            ts
                        ));
                    }
                }
            }
        );
        //TODO 9.开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS
            = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 10.聚合计算
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
            new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                @Override
                public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                    value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                    value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                    return value1;
                }
            },
            new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                    String start = DateFormatUtil.toYmdHms(window.getStart());
                    String end = DateFormatUtil.toYmdHms(window.getEnd());
                    for (TrafficHomeDetailPageViewBean viewBean : values) {
                        viewBean.setStt(start);
                        viewBean.setEdt(end);
                        viewBean.setTs(System.currentTimeMillis());
                        out.collect(viewBean);
                    }
                }
            }
        );
        //TODO 11.将聚合的结果写到Clickhouse中
        reduceDS.print(">>>");
        reduceDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_home_detail_page_view_window values(?,?,?,?,?)"));
        env.execute();
    }
}
