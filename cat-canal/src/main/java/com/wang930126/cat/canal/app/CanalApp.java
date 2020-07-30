package com.wang930126.cat.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalApp {
    public static void main(String[] args) {

        // 1.连接canal的服务端
        CanalConnector conn = CanalConnectors.newSingleConnector(
                new InetSocketAddress("spark105", 11111),//canal server 套接字地址
                "example",//destination 目的地名
                "",//canal客户端连接canal服务端用户名
                ""//canal客户端连接canal服务端密码
        );


        while(true){
            conn.connect(); //连接
            conn.subscribe("cat2019.*"); //订阅

            // TODO 2.抓取数据
            Message message = conn.get(100); //尝试获取100条sql的执行结果一个message中可能包含多个sql操作的执行结果集
/*            sql1
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5
            sql2
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5
                    c1 c2 c3 c4 c5*/
            if(message.getEntries().size() == 0){ //如果无变化 程序休眠
                System.out.println("目前没有数据，监控程序暂时休息5s");
                try {
                    Thread.currentThread().sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // TODO 3.数据获取 清洗
            }else{ //如果有数据变化 进行处理 一个entry对应一个sql执行的结果集 Entry:sql -> resultset
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if(entry.getEntryType() == CanalEntry.EntryType.ROWDATA){
                        ByteString storeValue = entry.getStoreValue(); //entry中的getsotrevalue方法获取数据 是一个字节字符串 需要反序列化
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);//工具类反序列化
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//返回一个List包含了本sql中所有变化的行数据
                        String tableName = entry.getHeader().getTableName();//表的名称
                        // TODO 4.sink到kafka 要求:订单增加发往订单主题 用户信息增加 用户信息更新 发往用户主题
                        // 需要创建一个类
                        // 类中包含字段信息:
                        //                  mysql发生变化表名tableName
                        //                  此次更新事件的类型eventType
                        //                  此次的所有变化的数据rowDataList
                        CanalHandler canalHandler = new CanalHandler(tableName, rowChange.getEventType(), rowDatasList);
                        canalHandler.handle(); //处理消息
                    }
                }
            }

        }

    }
}
