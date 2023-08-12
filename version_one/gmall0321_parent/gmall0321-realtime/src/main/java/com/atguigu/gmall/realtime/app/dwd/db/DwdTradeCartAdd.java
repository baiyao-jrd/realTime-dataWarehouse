package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MySqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2022/9/8
 * 交易域：加购事实表
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.检查点相关的设置(略)
        //TODO 3.从kafka的topic_db主题中读取数据 创建动态表
        String groupId = "dwd_trade_cart_add_group";
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL(groupId));
        // tableEnv.executeSql("select * from topic_db").print();

        //TODO 4.过滤出加购数据
        Table cartTable = tableEnv.sqlQuery("select \n" +
            "    data['id'] id,\n" +
            "    data['user_id'] user_id,\n" +
            "    data['sku_id'] sku_id,\n" +
            "    data['source_type'] source_type,\n" +
            "    ts,\n" +
            "    proc_time,\n" +
            "    if(\n" +
            "    `type`='insert',data['sku_num'],cast((CAST(data['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) as string)\n" +
            "    ) sku_num\n" +
            "from topic_db\n" +
            "where \n" +
            "    `table`='cart_info'  and \n" +
            "   (`type` = 'insert' or (`type`='update' and  `old`['sku_num'] is not null and CAST(data['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)))");

        tableEnv.createTemporaryView("cart_table",cartTable);
        // tableEnv.executeSql("select * from cart_table").print();

        //TODO 5.从MySQL数据库中获取字典表数据
        tableEnv.executeSql(MySqlUtil.getBaseDicLookUpDDL());

        // tableEnv.executeSql("select * from base_dic").print();
        //TODO 6.将加购表和字典表进行关联 --- 将字典维度退化到加购事实表中
        Table resTable = tableEnv.sqlQuery("SELECT cadd.id, cadd.user_id, cadd.sku_id,cadd.sku_num,cadd.source_type,dic.dic_name,cadd.ts\n" +
            "FROM cart_table cadd JOIN base_dic FOR SYSTEM_TIME AS OF cadd.proc_time AS dic \n" +
            "ON cadd.source_type = dic.dic_code");
        tableEnv.createTemporaryView("res_table",resTable);

        //TODO 7.创建动态表和要写入的kafka的主题建立映射关系
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
            "  id STRING,\n" +
            "  user_id STRING,\n" +
            "  sku_id STRING,\n" +
            "  sku_num STRING,\n" +
            "  source_type_code STRING,\n" +
            "  source_type_name STRING,\n" +
            "  ts STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") " + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cart_add"));

        //TODO 8.将关联的结果写到kafka的主题中
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from res_table");
    }
}
