package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DimUtil;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

/**
 * @author Felix
 * @date 2022/9/3
 * 将维度流中的数据写到phoenix表中
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource dataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //将流中的json写到phoenix对应的表中
        //{"tm_name":"banzhang","sink_table":"dim_base_trademark","id":12}
        String tableName = jsonObj.getString("sink_table");
        // {"tm_name":"banzhang","id":12}
        jsonObj.remove("sink_table");

        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        //拼接upsert语句   upsert into 表空间.表 (a,b,c) values(aa,bb,cc);
        String upsertSql = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + tableName
            + " (" + StringUtils.join(jsonObj.keySet(), ",") + ")" +
            " values('" + StringUtils.join(jsonObj.values(), "','") + "')";
        System.out.println("向phoenix表中插入数据的SQL:" + upsertSql);

        Connection conn = dataSource.getConnection();
        PhoenixUtil.executeSql(upsertSql,conn);

        //判断 如果是对维度表进行了修改，将Redis中缓存的维度数据清除掉
        if("update".equals(type)){
            DimUtil.delCached(tableName,jsonObj.getString("id"));
        }
    }
}
