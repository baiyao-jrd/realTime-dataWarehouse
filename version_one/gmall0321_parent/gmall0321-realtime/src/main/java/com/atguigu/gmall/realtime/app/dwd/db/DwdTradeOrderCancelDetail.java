package com.atguigu.gmall.realtime.app.dwd.db;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2022/9/8
 * 交易域：取消订单事实表
 */
public class DwdTradeOrderCancelDetail {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 读取 Kafka dwd_trade_order_pre_process 主题数据
        tableEnv.executeSql("" +
            "create table dwd_trade_order_pre_process(\n" +
            "id string,\n" +
            "order_id string,\n" +
            "user_id string,\n" +
            "order_status string,\n" +
            "sku_id string,\n" +
            "sku_name string,\n" +
            "province_id string,\n" +
            "activity_id string,\n" +
            "activity_rule_id string,\n" +
            "coupon_id string,\n" +
            "date_id string,\n" +
            "create_time string,\n" +
            "operate_date_id string,\n" +
            "operate_time string,\n" +
            "source_id string,\n" +
            "source_type string,\n" +
            "source_type_name string,\n" +
            "sku_num string,\n" +
            "split_original_amount string,\n" +
            "split_activity_amount string,\n" +
            "split_coupon_amount string,\n" +
            "split_total_amount string,\n" +
            "`type` string,\n" +
            "`old` map<string,string>,\n" +
            "od_ts string,\n" +
            "oi_ts string,\n" +
            "row_op_ts timestamp_ltz(3)\n" +
            ")" + MyKafkaUtil.getKafkaDDL(
            "dwd_trade_order_pre_process", "dwd_trade_cancel_detail"));

        // TODO 4. 筛选取消订单明细数据
        Table filteredTable = tableEnv.sqlQuery("" +
            "select\n" +
            "id,\n" +
            "order_id,\n" +
            "user_id,\n" +
            "sku_id,\n" +
            "sku_name,\n" +
            "province_id,\n" +
            "activity_id,\n" +
            "activity_rule_id,\n" +
            "coupon_id,\n" +
            "operate_date_id date_id,\n" +
            "operate_time cancel_time,\n" +
            "source_id,\n" +
            "source_type source_type_code,\n" +
            "source_type_name,\n" +
            "sku_num,\n" +
            "split_original_amount,\n" +
            "split_activity_amount,\n" +
            "split_coupon_amount,\n" +
            "split_total_amount,\n" +
            "oi_ts ts,\n" +
            "row_op_ts\n" +
            "from dwd_trade_order_pre_process\n" +
            "where `type` = 'update'\n" +
            "and `old`['order_status'] is not null\n" +
            "and order_status = '1003'");
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 5. 建立 Upsert-Kafka dwd_trade_cancel_detail 表
        tableEnv.executeSql("create table dwd_trade_cancel_detail(\n" +
            "id string,\n" +
            "order_id string,\n" +
            "user_id string,\n" +
            "sku_id string,\n" +
            "sku_name string,\n" +
            "province_id string,\n" +
            "activity_id string,\n" +
            "activity_rule_id string,\n" +
            "coupon_id string,\n" +
            "date_id string,\n" +
            "cancel_time string,\n" +
            "source_id string,\n" +
            "source_type_code string,\n" +
            "source_type_name string,\n" +
            "sku_num string,\n" +
            "split_original_amount string,\n" +
            "split_activity_amount string,\n" +
            "split_coupon_amount string,\n" +
            "split_total_amount string,\n" +
            "ts string,\n" +
            "row_op_ts timestamp_ltz(3),\n" +
            "primary key(id) not enforced\n" +
            ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_cancel_detail"));

        // TODO 6. 将数据写出到 Kafka
        tableEnv.executeSql(
            "insert into dwd_trade_cancel_detail select * from filtered_table");
    }

}
