package com.atguigu.gmall.publisher.controller;

import com.atguigu.gmall.publisher.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.publisher.service.TradeStatService;
import com.atguigu.gmall.publisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Felix
 * @date 2022/9/17
 * 交易域统计Controller
 */
@RestController
public class TradeStatController {

    @Autowired
    private TradeStatService tradeStatService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        //判断日期是否为null
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatService.getGMV(date);
        String json = "{\"status\": 0,\"data\": " + gmv + "}";
        return json;
    }

    /*@RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount orderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \""+orderAmount.getProvinceName()+"\",\"value\": "+orderAmount.getOrderAmount()+"}");
            if(i < provinceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }*/

    @RequestMapping("/province")
    public Map getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatService.getProvinceAmount(date);

        Map resMap = new HashMap();
        resMap.put("status",0);
        Map dataMap = new HashMap();
        List mapDataList = new ArrayList();
        for (TradeProvinceOrderAmount orderAmount : provinceOrderAmountList) {
            Map provinceMap = new HashMap();
            provinceMap.put("name",orderAmount.getProvinceName());
            provinceMap.put("value",orderAmount.getOrderAmount());
            mapDataList.add(provinceMap);
        }
        dataMap.put("mapData",mapDataList);
        dataMap.put("valueName","交易额");
        resMap.put("data",dataMap);
        return resMap;
    }

}
