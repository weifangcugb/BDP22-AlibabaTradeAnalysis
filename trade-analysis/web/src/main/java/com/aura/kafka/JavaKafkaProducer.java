package com.aura.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;


public class JavaKafkaProducer
{
    // config
    public Properties getConfig()
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "master:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    public void produceMessage()
    {
        Properties props = getConfig();
        Producer<String, String> producer = new KafkaProducer(props);
        String userPayFile = "trade-analysis/web/data/user_pay.txt";
        File file = new File(userPayFile);
        BufferedInputStream fis = null;
        BufferedReader reader = null;// 用10M的缓冲读取文本文件
        try {
            fis = new BufferedInputStream(new FileInputStream(file));
            reader = new BufferedReader(new InputStreamReader(fis,"utf-8"),10*1024*1024);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        String line = "";
        try {
            while((line = reader.readLine()) != null){
                String[] attr = line.split(",",-1);
                //topic为user_pay, user_id为key，shop_id+”,”+time_stamp为value
                producer.send(new ProducerRecord<String, String>("user_pay", attr[0], attr[1]+","+attr[2]));
                System.out.println(line);
                Thread.sleep(1000);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        producer.close();
    }

    public static void main(String[] args)
    {
        JavaKafkaProducer example = new JavaKafkaProducer();
        example.produceMessage();
    }
}
