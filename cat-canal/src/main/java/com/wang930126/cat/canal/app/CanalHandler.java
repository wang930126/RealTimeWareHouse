package com.wang930126.cat.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wang930126.cat.common.constants;

import java.util.List;

public class CanalHandler {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        //如果是新插入的订单表 发往Kafka订单主题
        if(eventType == CanalEntry.EventType.INSERT && "order_info".equals(tableName)){
            send2Kafka(constants.KAFKA_TOPIC_ORDER);
            //如果是新增或修改的用户表 发往Kafka用户主题
        }else if("user_info".equals(tableName) && (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE)){
            send2Kafka(constants.KAFKA_TOPIC_USER);
        }
    }

    public void send2Kafka(String topic){

        //rowData表示一行行的数据
        for (CanalEntry.RowData rowData : rowDataList) {
            JSONObject jsonObject = new JSONObject();
            //getAfterColumnsList方法获取columnList 表示每一行中一列列的数据
            List<CanalEntry.Column> columnList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : columnList) {
                jsonObject.put(column.getName(),column.getValue());
            }
//            System.out.println(jsonObject.toJSONString());
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }

    }


}
