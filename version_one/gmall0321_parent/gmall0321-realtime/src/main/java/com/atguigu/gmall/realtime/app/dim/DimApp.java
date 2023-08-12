package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Felix
 * @date 2022/9/2
 * DIM层维度处理
 * 需要启动的进程
 *      zk、kafka、hdfs、maxwell、hbase、DimApp
 * 开发流程
 *      基本环境准备
 *      检查点相关设置
 *      从kafka的topic_db主题中读取数据
 *      对读取的数据进行类型转换    jsonStr->jsonObj
 *      简单的ETL
 *      ~~~~~~~~~~~~~~~~~~~主流数据读取~~~~~~~~~~~~~~~~~~~~~~~~
 *      使用FlinkCDC从配置表中读取配置信息(需要提前将业务数据库的维度表配置到配置表中)
 *      将读取到的配置流进行广播
 *      ~~~~~~~~~~~~~~~~~~~配置数据读取~~~~~~~~~~~~~~~~~~~~~~~~
 *      使用connect算子将主流和广播流进行关联
 *      对关联之后的数据进行处理  process
 *          class TableProcessFunction extends BroadcastProcessFunction{
 *              processElement  处理主流数据
 *                  根据处理的业务数据库表的表名到广播状态中获取对应的配置信息
 *                  如果配置信息不为空，说明是维度
 *                      过滤掉不需要向下游传递的字段
 *                      补充sink_table属性
 *                      将data属性内容传递到下游
 *              processBroadcastElement  处理广播流数据
 *                  if(op=="d"){
 *                      将对象的配置信息从广播状态删除掉
 *                  }else{
 *                      提前创建维度表
 *                      将读取到的配置信息  放到广播状态中 <k:维度表表名  v:TableProcess对象>
 *                  }
 *          }
 *      将流中的数据写到phoenix表中
 *          class DimSinkFunction extends RichSinkFunction{
 *              invoke{
 *                  拼接upsert语句
 *                  执行upsert语句----封装工具类PhoenixUtil--executeSql
 *              }
 *          }
 *  执行流程
 *      运行DimApp
 *      从配置表中读取对应的配置信息，提前创建维度表
 *      将配置信息放到广播状态中存储起来
 *      对业务数据库的维度表进行操作
 *      binlog会记录业务数据库维度表的变化
 *      maxwell从binlog中读取业务数据库表的变化，并封装为json字符串，发送到kafka的topic_db主题中
 *      DimApp从topic_db主题中读取数据
 *      调用processElement方法处理读取到的数据
 *          根据当前处理的数据的表名从广播状态中获取对应的配置信息
 *          如果获取到，说明是维度，继续向下游传递；如果没有获取到对象的配置，什么也不做(过滤)
 *      将维度数据写到phoenix表
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*//TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");*/

        //TODO 3.从kafka的topic_db主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic,groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //TODO 4.对读取的数据进行类型转换       jsonStr->jsonObj
       /*
       //匿名内部类
       SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                return JSON.parseObject(jsonStr);
            }
        });
        //lambda表达式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(jsonStr->JSON.parseObject(jsonStr));
        */
       //方法的默认调用
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // jsonObjDS.print(">>>>");
        //TODO 5.简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
            new FilterFunction<JSONObject>() {
                @Override
                public boolean filter(JSONObject jsonObj) throws Exception {
                    try {
                        jsonObj.getJSONObject("data");
                        if (jsonObj.getString("type").equals("bootstrap-start")
                            || jsonObj.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            }
        );

        //TODO 6.使用FlinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop202")
            .port(3306)
            .databaseList("gmall0321_config") // set captured database
            .tableList("gmall0321_config.table_process") // set captured table
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

        DataStreamSource<String> mySqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //TODO 7.对读取到的配置流数据进行广播  得到广播流并定义广播状态
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
            = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor",String.class,TableProcess.class);
        BroadcastStream<String> broadcastDS = mySqlDS.broadcast(mapStateDescriptor);

        //TODO 8.将主流业务数据和广播流配置数据 进行关联   connect
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        //TODO 9.对关联之后的数据进行处理 得到维度数据流
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(
            new TableProcessFunction(mapStateDescriptor)
        );

        dimDS.print(">>>>");
        //TODO 10.将维度数据写到phoenix表中
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}