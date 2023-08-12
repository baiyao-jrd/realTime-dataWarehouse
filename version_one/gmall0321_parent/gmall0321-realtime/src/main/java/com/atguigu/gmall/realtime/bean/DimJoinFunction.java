package com.atguigu.gmall.realtime.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @author Felix
 * @date 2022/9/16
 * 维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void join(T obj, JSONObject dimInfoJsonObj) ;

   String getKey(T obj) ;
}
