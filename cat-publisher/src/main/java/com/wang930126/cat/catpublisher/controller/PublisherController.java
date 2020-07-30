package com.wang930126.cat.catpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wang930126.cat.catpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String realTimeHourDate(@RequestParam("date") String date){

        List<Map> data = new ArrayList<>();

        // TODO 1.利用mybatis生成的访问对象获取实时的UV：Int
        int dauTotal = publisherService.getDauTotal(date);
        HashMap<String, Object> map1 = new HashMap<>();
        map1.put("id","dau");
        map1.put("name","新增日活");
        map1.put("value",dauTotal);
        data.add(map1);

        // TODO 2.添加mid新增
        HashMap<String, Object> map2 = new HashMap<>();
        map2.put("id","new_mid");
        map2.put("name","新增用户");
        map2.put("value",233);
        data.add(map2);

        // TODO 3.返回JSON字符串如下
        // [{"id":"dau","name":"新增日活","value":1200},{"id":"new_mid","name":"新增设备","value":233}]

        // TODO 4.添加order_amount新增交易额
        Double orderAmount = publisherService.getOrderAmount(date);
        HashMap<String, Object> map3 = new HashMap<>();
        map3.put("id","order_amount");
        map3.put("name","新增交易额");
        map3.put("value",orderAmount);
        data.add(map3);

        // TODO 5.返回的JSON字符串如下
        //[{"name":"新增日活","id":"dau","value":0},{"name":"新增用户","id":"new_mid","value":233},{"name":"新增交易额","id":"order_amount","value":52489.0}]

        return JSON.toJSONString(data);

    }

    @GetMapping("realtime-hour")
    public String realTimeHourDate(@RequestParam("date") String date,@RequestParam("type") String type){

        if("dau".equals(type)){

            // Map("11"->123,"12"->566.。)
            // TODO 1.存入今日的数据 "today":{"12":38,"13":1233,"17":123,"19":688}
            Map dauHour = publisherService.getDauHour(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",dauHour);

            // TODO 2.存入昨日数据 "yesterday:"{"11":383,"12":323,"17":88,"19":200}
            String yesterDayString = "";
            try {
                Date todayDate = new SimpleDateFormat("yyyy-MM-dd").parse(date);
                Date yesterdayDate = DateUtils.addDays(todayDate, -1);
                yesterDayString = new SimpleDateFormat("yyyy-MM-dd").format(yesterdayDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Map dauHoursYesterday = publisherService.getDauHour(yesterDayString);
            jsonObject.put("yesterday",dauHoursYesterday);

            // TODO 3.返回JSON字符串如下
            // {"yesterday:"{"11":383,"12":323,"17":88,"19":200},"today":{"12":38,"13":1233,"17":123,"19":688}}
            return jsonObject.toJSONString();
        }else if("order".equals(type)){

            // TODO 1.获取今日的数据 {"01":1234,"02":5678.。。} 并存入JSON字符串
            Map orderAmountHour = publisherService.getOrderAmountHour(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",orderAmountHour);

            // TODO 2.获取昨日的数据并存入JSON字符串
            String yesterdayString = null;
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date todayDate = sdf.parse(date);
                Date yesterdayDate = DateUtils.addDays(todayDate, -1);
                yesterdayString = sdf.format(yesterdayDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            Map orderAmountHourYest = publisherService.getOrderAmountHour(yesterdayString);
            jsonObject.put("yesterday",orderAmountHourYest);

            return jsonObject.toJSONString();
        }else{
            return null;
        }

    }

}
