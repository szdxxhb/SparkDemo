package com.xhb.hbase;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;


/**
 * Created by 16060764 on 2016/12/19.
 */
public class kafkaToHbase {
    public final static String  bootstrapServers="cdhslave01:9092,cdhslave02:9092,cdhslave03:9092";
    public final static String SplitCharactor = "\t";
    //插入数据orderinfo列族
    public final static String[] orderInfoColumns = { "userid", "sendpay", "paydate" };
    //插入数据moneyinfo列族
    public final static String[] moneyInfoColumns = { "orderamt", "orderyouhui"  };

    public final static HbaseUtils hu=new HbaseUtils();
    public final static String tableName="order_detail";

    public   void connectionKafka() {

        Random r = new Random();
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "test-topicConsumer-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                //System.out.println(record.value());
                System.out.println("offset = "+record.offset()+", key = "+record.key()+", value = "+record.value());
                //插入Hbase
                String rowkey =record.key();
                String[] values=record.value().split(SplitCharactor);

                if (values.length==6)
                {
                    //插入数据orderinfo列族
                    String[] orderInfoValues={ values[1],values[4],values[5] };
                    //插入数据moneyinfo列族
                    String[] moneyInfoValues = { values[2], values[3]};
                    try {
                        hu.addData(rowkey,tableName,orderInfoColumns,orderInfoValues,moneyInfoColumns,moneyInfoValues);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


            }



        }
    }

    }
