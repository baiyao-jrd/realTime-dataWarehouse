package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyClickhouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2022/9/14
 * 交易域：sku粒度下单聚合统计
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis、Clickhouse
 *      DwdTradeOrderPreProcess、DwdTradeOrderDetail、DwsTradeSkuOrderWindow
 * 开发流程
 *      基本环境准备
 *      检查点相关设置
 *      从下单事实表中读取下单数据
 *      对读取的数据进行类型转换    jsonStr->jsonObj
 *      按照订单明细的id进行分组
 *      使用Flink的状态编程 + 定时器 实现去重
 *      类型转换    jsonObj->实体类对象
 *      按照用户id进行分组
 *      判断是否为下单独立访客
 *      按照skuid进行分组
 *      开窗
 *      聚合计算
 *      和Sku维度进行关联
 *          -基本的实现
 *              PhoenixUtil->   List<T> queryList(conn,sql,clz)
 *              DimUtil-> JSONObject getDimInfoNoCache(conn,tableName,Tuple2...params)
 *          -优化1:旁路缓存
 *              思路：先从缓存中查询维度数据，如果缓存中存在要查找的维度数据，直接将维度返回(缓存命中)；
 *                    如果缓存中没有找到对应的维度，再发送请求到phoenix表中查询维度，并将查出的维度
 *                    放缓存中缓存起来，方便下次查找
 *              选型：Redis √       状态
 *              类型：String
 *              ex：1day
 *              注意：在业务数据维度数据发生变化的时候，将缓存的维度数据删除掉
 *          -优化2:异步IO
 *              要想提升对流中数据的处理能力，可以将并行度调大，但是调大并行度需要更多的硬件资源，不可能
 *              无限制的调整，所以在有限的硬件资源下，可以考虑使用异步的方式进行处理。
 *              语法：
 *                  AsyncDataStream.[un]orderedWait(
 *                      流,
 *                      异步操作 implements AsyncFunction(asyncInvoke),
 *                      超时时间,
 *                      时间单位
 *                  );
 *                  class DimAsyncFunction extends RichAsyncFunction{
 *                      open{
 *                          连接池；
 *                          线程池；
 *                      }
 *                      asyncInvoke{
 *                          开启多个线程，发送异步请求
 *                      }
 *                  }
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的配置(略)
        //TODO 3.从kafka的下单主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_sku_order_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        //TODO 4.将读取的数据进行类型的转换  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // {"create_time":"2022-09-01 10:05:28","sku_num":"2","activity_rule_id":"2",
        // "split_original_amount":"16394.0000","sku_id":"11","date_id":"2022-09-01","source_type_name":"智能推荐",
        // "user_id":"65","province_id":"1","source_type_code":"2403","row_op_ts":"2022-09-14 02:05:30.085Z",
        // "activity_id":"1","sku_name":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机","id":"239",
        // "order_id":"109","split_activity_amount":"1200.0","split_total_amount":"15194.0","ts":"1663121128"}
        // jsonObjDS.print(">>>");

        //TODO 5.按照唯一键order_detail_id进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //TODO 6.使用Flink的状态编程 + 定时器去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<JSONObject> lastJsonObjState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<JSONObject> valueStateDescriptor
                        = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                    lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject lastJsonObj = lastJsonObjState.value();
                    if (lastJsonObj == null) {
                        lastJsonObjState.update(jsonObj);
                        //注册定时器
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                    } else {
                        //如果状态中已经存在了数据 ，用当前数据的时间戳和状态中的数据时间戳进行比较，
                        //将时间戳大的保留到状态中   "row_op_ts":"2022-09-14 02:05:30.085Z"
                        String lastRowOpTs = lastJsonObj.getString("row_op_ts");
                        String curRowOpTs = jsonObj.getString("row_op_ts");
                        if (TimestampLtz3CompareUtil.compare(lastRowOpTs, curRowOpTs) <= 0) {
                            lastJsonObjState.update(jsonObj);
                        }
                    }
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                    JSONObject jsonObj = lastJsonObjState.value();
                    if (jsonObj != null) {
                        out.collect(jsonObj);
                    }
                    lastJsonObjState.clear();
                }
            }
        );
        //TODO 7.再次进行类型转换   jsonObj->实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> orderBeanDS = distinctDS.map(
            new MapFunction<JSONObject, TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Double splitOriginalAmount = jsonObj.getDouble("split_original_amount");
                    Double splitActivityAmount = jsonObj.getDouble("split_activity_amount");
                    Double splitCouponAmount = jsonObj.getDouble("split_coupon_amount");
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    Long ts = jsonObj.getLong("ts") * 1000L;

                    TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                        .orderIdSet(new HashSet<String>(
                            Collections.singleton(orderId)
                        ))
                        .skuId(skuId)
                        .userId(userId)
                        .orderUuCount(0L)
                        .originalAmount(splitOriginalAmount)
                        .activityAmount(splitActivityAmount == null ? 0.0 : splitActivityAmount)
                        .couponAmount(splitCouponAmount == null ? 0.0 : splitCouponAmount)
                        .orderAmount(splitTotalAmount)
                        .ts(ts)
                        .build();
                    return orderBean;
                }
            }
        );
        // orderBeanDS.print("$$$$");
        //TODO 8.按照用户id进行分组
        KeyedStream<TradeSkuOrderBean, String> userIdKeyedDS = orderBeanDS.keyBy(TradeSkuOrderBean::getUserId);
        //TODO 9.使用Flink的状态编程，判断下单独立用户
        SingleOutputStreamOperator<TradeSkuOrderBean> orderUUDS = userIdKeyedDS.process(
            new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {
                private ValueState<String> lastOrderDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastOrderDateState", String.class);
                    valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    lastOrderDateState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public void processElement(TradeSkuOrderBean orderBean, Context ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                    String lastOrderDate = lastOrderDateState.value();
                    String curOrderDate = DateFormatUtil.toDate(orderBean.getTs());
                    if (StringUtils.isEmpty(lastOrderDate) || !lastOrderDate.equals(curOrderDate)) {
                        orderBean.setOrderUuCount(1L);
                        lastOrderDateState.update(curOrderDate);
                    }
                    out.collect(orderBean);
                }
            }
        );
        //TODO 10.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS = orderUUDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                        @Override
                        public long extractTimestamp(TradeSkuOrderBean orderBean, long recordTimestamp) {
                            return orderBean.getTs();
                        }
                    }
                )
        );
        // withWatermarkDS.print(">>>>");

        //TODO 11.按照sku维度进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = withWatermarkDS.keyBy(TradeSkuOrderBean::getSkuId);

        //TODO 12.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS
            = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 13.聚合计算
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
            new ReduceFunction<TradeSkuOrderBean>() {
                @Override
                public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                    value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                    value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                    value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                    value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                    value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                    value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                    return value1;
                }
            },
            new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                    String start = DateFormatUtil.toYmdHms(window.getStart());
                    String end = DateFormatUtil.toYmdHms(window.getEnd());
                    for (TradeSkuOrderBean orderBean : input) {
                        orderBean.setStt(start);
                        orderBean.setEdt(end);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        orderBean.setTs(System.currentTimeMillis());
                        out.collect(orderBean);
                    }
                }
            }
        );
        // TradeSkuOrderBean(stt=2022-09-15 14:41:10, edt=2022-09-15 14:41:20, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, orderIdSet=[129], userId=34, skuId=12, skuName=null, spuId=null, spuName=null, orderUuCount=1, orderCount=1, originalAmount=18394.0, activityAmount=1200.0, couponAmount=0.0, orderAmount=17194.0, ts=1663224110149)
        // reduceDS.print(">>>>");
        //TODO 14.和sku维度进行关联
        //将异步I/O操作应用于DataStream作为DataStream的一次转换操作
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
            reduceDS,
            //实现分发请求的AsyncFunction
            new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info") {
                @Override
                public void join(TradeSkuOrderBean orderBean, JSONObject dimInfoJsonObj) {
                    orderBean.setSkuName(dimInfoJsonObj.getString("SKU_NAME"));
                    orderBean.setTrademarkId(dimInfoJsonObj.getString("TM_ID"));
                    orderBean.setCategory3Id(dimInfoJsonObj.getString("CATEGORY3_ID"));
                    orderBean.setSpuId(dimInfoJsonObj.getString("SPU_ID"));
                }

                @Override
                public String getKey(TradeSkuOrderBean orderBean) {
                    return orderBean.getSkuId();
                }
            },
            60,
            TimeUnit.SECONDS
        );
        // withSkuInfoDS.print(">>>>");

        //TODO 15.和tm维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
            withSkuInfoDS,
            new DimAsyncFunction<TradeSkuOrderBean>("dim_base_trademark") {
                @Override
                public void join(TradeSkuOrderBean orderBean, JSONObject dimInfoJsonObj) {
                    orderBean.setTrademarkName(dimInfoJsonObj.getString("TM_NAME"));
                }

                @Override
                public String getKey(TradeSkuOrderBean orderBean) {
                    return orderBean.getTrademarkId();
                }
            },
            60, TimeUnit.SECONDS
        );

        // withTmDS.print(">>>>");

        //TODO 16.和spu维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
            withTmDS,
            new DimAsyncFunction<TradeSkuOrderBean>("dim_spu_info") {
                @Override
                public void join(TradeSkuOrderBean orderBean, JSONObject dimInfoJsonObj) {
                    orderBean.setSpuName(dimInfoJsonObj.getString("SPU_NAME"));
                }

                @Override
                public String getKey(TradeSkuOrderBean orderBean) {
                    return orderBean.getSpuId();
                }
            },
            60,
            TimeUnit.SECONDS
        );
        // withSpuInfoDS.print(">>>>");
        //TODO 17.和category3维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
            withSpuInfoDS,
            new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category3".toUpperCase()) {
                @Override
                public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj){
                    javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                    javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                }

                @Override
                public String getKey(TradeSkuOrderBean javaBean) {
                    return javaBean.getCategory3Id();
                }
            },
            5 * 60, TimeUnit.SECONDS
        );

        //TODO 18.和category2维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
            withCategory3Stream,
            new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category2".toUpperCase()) {
                @Override
                public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj) {
                    javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                    javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                }

                @Override
                public String getKey(TradeSkuOrderBean javaBean) {
                    return javaBean.getCategory2Id();
                }
            },
            5 * 60, TimeUnit.SECONDS
        );

        //TODO 19.和category1维度进行关联
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
            withCategory2Stream,
            new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category1".toUpperCase()) {
                @Override
                public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj){
                    javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                }

                @Override
                public String getKey(TradeSkuOrderBean javaBean) {
                    return javaBean.getCategory1Id();
                }
            },
            5 * 60, TimeUnit.SECONDS
        );

        withCategory1Stream.print(">>>>");
        //TODO 20.将关联的结果写到CK中
        withCategory1Stream.addSink(MyClickhouseUtil.getSinkFunction("insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
