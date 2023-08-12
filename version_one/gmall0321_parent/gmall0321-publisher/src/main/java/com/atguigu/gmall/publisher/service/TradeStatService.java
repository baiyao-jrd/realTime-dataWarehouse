package com.atguigu.gmall.publisher.service;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2022/9/17
 * 交易域统计service接口
 */
public interface TradeStatService {
    //获取某一天总交易额
    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
