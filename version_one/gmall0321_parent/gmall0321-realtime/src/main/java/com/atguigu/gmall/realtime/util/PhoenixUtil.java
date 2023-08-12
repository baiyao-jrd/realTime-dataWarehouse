package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2022/9/3
 * 操作phoenix的工具类
 */
public class PhoenixUtil {
    public static void executeSql(String sql, Connection conn) {
        PreparedStatement ps = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //从phoenix数据库表中查询数据
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            // 处理结果集
            /*
            +-----+-------+------------+------------+-----------+-------------+
            | ID  | NAME  | REGION_ID  | AREA_CODE  | ISO_CODE  | ISO_3166_2  |
            +-----+-------+------------+------------+-----------+-------------+
            | 1   | 北京    | 1          | 110000     | CN-11     | CN-BJ      |
            | 10  | 福建    | 2          | 350000     | CN-35     | CN-FJ      |
            */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //定义一个对象，用于封装查询出来的每一条记录
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix表中查询维度数据发生了异常");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) throws SQLException {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection conn = dataSource.getConnection();
        System.out.println(queryList(conn, "select * from GMALL0321_SCHEMA.DIM_BASE_TRADEMARK", JSONObject.class));
    }
}
