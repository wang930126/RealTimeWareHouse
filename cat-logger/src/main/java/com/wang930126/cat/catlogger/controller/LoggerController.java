package com.wang930126.cat.catlogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wang930126.cat.common.constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @PostMapping("/log")
    @ResponseBody
    public String log(@RequestParam("logString") String logString){
        JSONObject obj = JSON.parseObject(logString);
        obj.put("ts",System.currentTimeMillis());
        log.info(obj.toJSONString());
        if("startup".equals(obj.get("type"))){
            kafkaTemplate.send(constants.KAFKA_TOPIC_STARTUP,obj.toJSONString());
        }else{
            kafkaTemplate.send(constants.KAFKA_TOPIC_EVENT,obj.toJSONString());
        }
        return "success";
    }

}
