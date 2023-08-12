package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Felix
 * @date 2022/9/9
 * 流量域：版本、渠道、地区、新老访客聚合统计
 * 需要启动的进程
 * zk、kafka、flume、clickhouse、DwdTrafficBaseLogSplit、DwdTrafficUniqueVisitorDetail
 * DwdTrafficUserJumpDetail、DwsTrafficVcChArIsNewPageViewWindow
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)
        /*//2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30),org.apache.flink.api.common.time.Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
*/
        //TODO 3.从kafka主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String groupId = "dws_traffic_vc_ch_group";
        String pageLogTopic = "dwd_traffic_page_log";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujdTopic = "dwd_traffic_user_jump_detail";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> pageLogKafkaConsumer = MyKafkaUtil.getKafkaConsumer(pageLogTopic, groupId);
        FlinkKafkaConsumer<String> uvKafkaConsumer = MyKafkaUtil.getKafkaConsumer(uvTopic, groupId);
        FlinkKafkaConsumer<String> ujdKafkaConsumer = MyKafkaUtil.getKafkaConsumer(ujdTopic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> pageLogStrDS = env.addSource(pageLogKafkaConsumer);
        DataStreamSource<String> uvStrDS = env.addSource(uvKafkaConsumer);
        DataStreamSource<String> ujdStrDS = env.addSource(ujdKafkaConsumer);
        //TODO 4.对读取的数据进行类型转换   jsonStr->实体类对象
        //4.1 pageLog
        SingleOutputStreamOperator<TrafficPageViewBean> pageLogStatsDS = pageLogStrDS.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String jsonStr) throws Exception {
                //将json字符串转换为json对象
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                Long ts = jsonObj.getLong("ts");
                TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                    "",
                    "",
                    commonJsonObj.getString("vc"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("is_new"),
                    0L,
                    StringUtils.isEmpty(pageJsonObj.getString("last_page_id")) ? 1L : 0L,
                    1L,
                    pageJsonObj.getLong("during_time"),
                    0L,
                    ts
                );
                return pageViewBean;
            }
        });
        //4.2 uv
        SingleOutputStreamOperator<TrafficPageViewBean> uvStatsDS = uvStrDS.map(
            new MapFunction<String, TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        ts
                    );
                    return pageViewBean;
                }
            }
        );
        //4.3 ujd
        SingleOutputStreamOperator<TrafficPageViewBean> ujdStatsDS = ujdStrDS.map(
            new MapFunction<String, TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean map(String jsonStr) throws Exception {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");

                    TrafficPageViewBean pageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        commonJsonObj.getString("vc"),
                        commonJsonObj.getString("ch"),
                        commonJsonObj.getString("ar"),
                        commonJsonObj.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        0L,
                        1L,
                        ts
                    );
                    return pageViewBean;
                }
            }
        );
        //TODO 5.使用union将3条流进行合并
        DataStream<TrafficPageViewBean> unionDS = pageLogStatsDS.union(uvStatsDS, ujdStatsDS);
        // unionDS.print(">>>>");
        //TODO 6.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TrafficPageViewBean>() {
                        @Override
                        public long extractTimestamp(TrafficPageViewBean viewBean, long recordTimestamp) {
                            return viewBean.getTs();
                        }
                    }
                )
        );
        //TODO 7.分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedDS = withWatermarkDS.keyBy(
            new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(TrafficPageViewBean viewBean) throws Exception {
                    return Tuple4.of(
                        viewBean.getVc(),
                        viewBean.getCh(),
                        viewBean.getAr(),
                        viewBean.getIsNew()
                    );
                }
            }
        );
        //TODO 8.开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
            = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 9.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS = windowDS.reduce(
            new ReduceFunction<TrafficPageViewBean>() {
                @Override
                public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                    value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                    value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                    value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                    value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                    value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                    return value1;
                }
            },
            new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                @Override
                public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                    String start = DateFormatUtil.toYmdHms(window.getStart());
                    String end = DateFormatUtil.toYmdHms(window.getEnd());
                    for (TrafficPageViewBean viewBean : input) {
                        viewBean.setStt(start);
                        viewBean.setEdt(end);
                        viewBean.setTs(System.currentTimeMillis());
                        out.collect(viewBean);
                    }
                }
            }
        );

        //TODO 10.将聚合的结果写到Clickhouse
        reduceDS.print(">>>>");
        reduceDS.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
