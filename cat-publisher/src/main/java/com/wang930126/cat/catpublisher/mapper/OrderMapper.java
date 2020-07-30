package com.wang930126.cat.catpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    // 获得当日的订单的总交易额
    public Double selectOrderTotal(String date);

    // 获得当日的订单的分时交易总额 [{"CREATE_HOUR":04,"SUM_AMOUNT":831.0},{},{}。。。]
    public List<Map> selectOrderTotalHourMap(String date);

}
