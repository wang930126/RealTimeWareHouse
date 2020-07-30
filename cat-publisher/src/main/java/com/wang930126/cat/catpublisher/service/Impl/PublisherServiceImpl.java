package com.wang930126.cat.catpublisher.service.Impl;

import com.wang930126.cat.catpublisher.mapper.DauMapper;
import com.wang930126.cat.catpublisher.mapper.OrderMapper;
import com.wang930126.cat.catpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHour(String date) {
        HashMap dauHourMap = new HashMap();
        // [{"LH":11,"CT":233},{"LH":12,"CT":566}。。]
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"),map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        Map<String, Object> map = new HashMap<>();
        List<Map> maps = orderMapper.selectOrderTotalHourMap(date);
        for (Map map1 : maps) {
            map.put((String)map1.get("CREATE_HOUR"),map1.get("SUM_AMOUNT"));
        }
        return map;
    }
}
