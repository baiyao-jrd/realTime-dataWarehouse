package com.atguigu.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Felix
 * @date 2022/9/4
 * 流量域：日志分流
 * 需要启动的进程
 *      zk、kafka、flume、DwdTrafficBaseLogSplit
 * 执行流程
 *      运行模拟生成日志数据的jar
 *      将生成的日志数据进行落盘到hadoop202以及hadoop203
 *      flume从磁盘上读取日志数据  发送到kafka的topic_log主题中
 *      DwdTrafficBaseLogSplit从topic_log主题中读取日志数据
 *          -ETL
 *          -新老访客标记修复
 *          -分流
 *          -将不同流中的数据写到kafka的不同的主题中(事实表)
 */
public class DwdTrafficBaseLogSplit {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 job取消之后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 3.从kafka的topic_log主题中读取日志数据
        //3.1 声明消费的主题消费者组
        String topic = "topic_log";
        String groupId = "dwd_traffic_log_split_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        // kafkaStrDS.print(">>>");

        //TODO 4.对流中的数据类型进行转换以及进行简单的ETL
        //4.1 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        //4.2 转换以及ETL  脏数据放到侧输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
            new ProcessFunction<String, JSONObject>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                    try {
                        //如果执行转换操作没有发生异常，说明是标准的json字符串，转换为对象后，继续向下游传递
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        out.collect(jsonObj);
                    } catch (Exception e) {
                        System.out.println("从topic_log主题中发现了脏数据");
                        //如果在转换的过程中，发生了异常，说明不是标准的json字符串，属于脏数据，放到侧输出流中
                        ctx.output(dirtyTag, jsonStr);
                    }
                }
            }
        );
        //4.3 将侧输出流数据 写到kafka专门的主题中
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);

        // jsonObjDS.print(">>>");
        // dirtyDS.print("$$$");
        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));

        //TODO 5.修复新老访客标记
        //5.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //5.2 使用Flink的状态编程 对新老访客标记进行修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
            new RichMapFunction<JSONObject, JSONObject>() {
                //注意：不能在声明的时候直接进行初始化
                private ValueState<String> lastVisitDateState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    lastVisitDateState
                        = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDateState", String.class));
                }

                @Override
                public JSONObject map(JSONObject jsonObj) throws Exception {
                    //获取新老访客标记
                    String isNew = jsonObj.getJSONObject("common").getString("is_new");
                    String lastVisitDate = lastVisitDateState.value();
                    Long ts = jsonObj.getLong("ts");

                    String curVisitDate = DateFormatUtil.toDate(ts);

                    if ("1".equals(isNew)) {
                        if (StringUtils.isEmpty(lastVisitDate)) {
                            lastVisitDateState.update(curVisitDate);
                        } else {
                            if (!lastVisitDate.equals(curVisitDate)) {
                                isNew = "0";
                                jsonObj.getJSONObject("common").put("is_new", isNew);
                            }
                        }
                    } else {
                        if (StringUtils.isEmpty(lastVisitDate)) {
                            String yesterDay = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                            lastVisitDateState.update(yesterDay);
                        }
                    }
                    return jsonObj;
                }
            }
        );
        // fixedDS.print(">>>>");

        //TODO 6.分流  错误日志-错误侧输出流   启动日志-启动侧输出流  曝光日志-曝光侧输出流  动作日志-动作侧输出流  页面日志-主流
        //6.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        //6.2 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
            new ProcessFunction<JSONObject, String>() {
                @Override
                public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                    //判断是否为错误日志
                    JSONObject errJsonObj = jsonObj.getJSONObject("err");
                    if (errJsonObj != null) {
                        //将错误日志 输出到错误侧输出流
                        ctx.output(errTag, jsonObj.toJSONString());
                        jsonObj.remove("err");
                    }
                    //判断是否为启动日志
                    JSONObject startJsonObj = jsonObj.getJSONObject("start");
                    if (startJsonObj != null) {
                        //将启动日志 输出到启动侧输出流
                        ctx.output(startTag, jsonObj.toJSONString());
                    } else {
                        //获取页面日志的三个基础属性
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        Long ts = jsonObj.getLong("ts");

                        //判断页面上是否存在曝光
                        JSONArray displayArr = jsonObj.getJSONArray("displays");
                        if (displayArr != null && displayArr.size() > 0) {
                            //遍历曝光数据
                            for (int i = 0; i < displayArr.size(); i++) {
                                //每遍历一次 得到一条曝光内容
                                JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                //创建一个新的对象  用于封装曝光内容
                                JSONObject newDisplayJsonObj = new JSONObject();
                                newDisplayJsonObj.put("common", commonJsonObj);
                                newDisplayJsonObj.put("page", pageJsonObj);
                                newDisplayJsonObj.put("ts", ts);
                                newDisplayJsonObj.put("display", displayJsonObj);
                                //将曝光日志 输出到曝光侧输出流
                                ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                            }
                        }
                        //判断页面上是否存在动作
                        JSONArray actionArr = jsonObj.getJSONArray("actions");
                        if (actionArr != null && actionArr.size() > 0) {
                            for (int i = 0; i < actionArr.size(); i++) {
                                JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                JSONObject newActionJsonObj = new JSONObject();
                                newActionJsonObj.put("common", commonJsonObj);
                                newActionJsonObj.put("page", pageJsonObj);
                                newActionJsonObj.put("action", actionJsonObj);
                                //将动作日志  输出到动作侧输出流
                                ctx.output(actionTag, newActionJsonObj.toJSONString());
                            }
                        }

                        //将页面日志输出到主流
                        jsonObj.remove("displays");
                        jsonObj.remove("actions");
                        out.collect(jsonObj.toJSONString());
                    }
                }
            }
        );

        //TODO 7.将不同流的数据写到kafka的不同的主题中
        DataStream<String> errDS = pageDS.getSideOutput(errTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print(">>>>");
        startDS.print("####");
        errDS.print("@@@@");
        displayDS.print("$$$$");
        actionDS.print("***");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        errDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_err_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));

        env.execute();
    }
}
