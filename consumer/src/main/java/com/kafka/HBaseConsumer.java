package com.kafka;

import com.dao.HbaseDAO;
import com.utils.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;

/**
 * @author AnLuyao
 * @date 2018-06-01 15:28
 */
public class HBaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Collections.singletonList(PropertiesUtil.getProperty("kafka.topic")));
        HbaseDAO hBaseDAO = new HbaseDAO();
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                String ori = record.value();
                System.out.println(ori);
                hBaseDAO.put(ori);
            }
        }
    }
}
