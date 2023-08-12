package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.publisher.mapper.TradeStatMapper;
import com.atguigu.gmall.publisher.service.TradeStatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2022/9/17
 * 交易域统计Service接口实现类
 */
@Service
public class TradeStatServiceImpl implements TradeStatService {
    @Autowired
    private TradeStatMapper tradeStatMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatMapper.selectProvinceAmount(date);
    }
}
