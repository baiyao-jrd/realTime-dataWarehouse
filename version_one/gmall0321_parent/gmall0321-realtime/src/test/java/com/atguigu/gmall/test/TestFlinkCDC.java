package com.atguigu.gmall.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Felix
 * @date 2022/9/2
 * 该案例演示了FlinkCDC的用法-API
 */
public class TestFlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop202")
            .port(3306)
            .databaseList("gmall0321_config") // set captured database
            .tableList("gmall0321_config.t_user") // set captured table
            .username("root")
            .password("123456")
            .startupOptions(StartupOptions.initial())
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();


        env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

// {"before":null,"after":{"id":1,"name":"cls","age":18},"source":{"version":"1.5.4.Final","connector":"mysql","name*/":"mysql_binlog_source","ts_ms":1662083037105,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"t_user","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1662083037110,"transaction":null}
// {"before":null,"after":{"id":3,"name":"lzls","age":25},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662084354000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":368,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1662084354816,"transaction":null}
// {"before":{"id":3,"name":"lzls","age":25},"after":{"id":3,"name":"lzls","age":27},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662084383000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":663,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1662084383498,"transaction":null}
// {"before":{"id":3,"name":"lzls","age":27},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662084430000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"t_user","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":974,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1662084430189,"transaction":null}

