package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Felix
 * @date 2022/9/2
 * 处理维度数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    private DruidDataSource dataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
    }

    //处理主流中的业务数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取当前处理业务数据库表的表名
        String tableName = jsonObj.getString("table");
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //根据业务数据库表表名从广播状态中获取配置信息
        TableProcess tableProcess = broadcastState.get(tableName);
        if(tableProcess != null){
            //说明是维度   将维度数据中data内容 继续向下游传递  {"tm_name":"banzhang","logo_url":"aaa","id":12}
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //将不需要向下游传递的字段 过滤掉   {"tm_name":"banzhang","id":12}
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(dataJsonObj,sinkColumns);

            //获取维度数据输出的目的地   {"tm_name":"banzhang","sink_table":"dim_base_trademark","id":12}
            String sinkTable = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table",sinkTable);

            dataJsonObj.put("type",jsonObj.getString("type"));

            out.collect(dataJsonObj);
        }
    }

    //过滤不需要向下游传递的字段
    // dataJsonObj :{"tm_name":"banzhang","logo_url":"aaa","id":12}
    // sinkColumns : id,tm_name
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry->!columnList.contains(entry.getKey()));
    }

    // {"before":null,"after":{"source_table":"base_trademark","sink_table":"dim_base_trademark","sink_columns":"id,tm_name","sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103141349,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1662103141349,"transaction":null}
    // {"before":null,"after":{"source_table":"aa","sink_table":"aaa","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103238000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":801824,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1662103238827,"transaction":null}
    // {"before":{"source_table":"aa","sink_table":"aaa","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"after":{"source_table":"aa","sink_table":"abb","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103282000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":802139,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1662103282838,"transaction":null}
    // {"before":{"source_table":"aa","sink_table":"abb","sink_columns":"a,b","sink_pk":"a","sink_extend":null},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1662103312000,"snapshot":"false","db":"gmall0321_config","sequence":null,"table":"table_process","server_id":1,"gtid":null,"file":"mysql-bin.000013","pos":802473,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1662103312677,"transaction":null}
    //处理广播流中的数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        //为了操作方便  将jsonStr转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String op = jsonObj.getString("op");
        //获取广播状态

        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //判断对配置的操作类型
        if ("d".equals(op)) {
            //如果对配置表进行删除操作    从广播状态中将配置信息删掉
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            broadcastState.remove(before.getSourceTable());
        } else {
            //如果对配置表进行读取、添加  将信息添加到广播状态；如果对配置表进行修改操作，将修改更新到广播状态中
            TableProcess after = jsonObj.getObject("after", TableProcess.class);

            //获取输出到phoenix的维度表表名
            String sinkTable = after.getSinkTable();
            //获取维度表中的字段
            String sinkColumns = after.getSinkColumns();
            //获取维度表的主键
            String sinkPk = after.getSinkPk();
            //获取建表扩展
            String sinkExtend = after.getSinkExtend();

            //在phoenix中提前创建维度表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

            //将配置信息放到广播状态中
            broadcastState.put(after.getSourceTable(), after);
        }
    }


    private void checkTable(String tableName, String sinkColumns, String pk, String ext) {
        //空值处理
        if (pk == null) {
            pk = "id";
        }
        if (ext == null) {
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + "(");

        String[] columnArr = sinkColumns.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            //判断是否是主键
            if(column.equals(pk)){
                createSql.append(column + " varchar primary key");
            }else{
                createSql.append(column + " varchar");
            }
            //判断是否是最后一个
            if(i < columnArr.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);
        System.out.println("在phoenix中执行的建表语句:" + createSql);

        Connection conn = null;
        try {
            //获取连接
            conn = dataSource.getConnection();
            PhoenixUtil.executeSql(createSql.toString(),conn);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
