package com.atguigu.gmall.publisher.mapper;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2022/9/17
 * 交易域统计Mapper接口
 */
public interface TradeStatMapper {
    //获取某一天总交易额
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    BigDecimal selectGMV(Integer date);

    //获取某一天各个省份交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date} group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);

}
