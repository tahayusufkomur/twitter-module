package com.github.kafka.twitter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());
        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "twitter_consumer";
        String topic = "twitter_topic";


        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offset);

        KafkaConsumer<String,String> consumer =
                new KafkaConsumer(properties);

        // subscribe consumer to our topics
        consumer.subscribe(Arrays.asList(topic));

        while(true) {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record: records){
                logger.info("Key: " + record.key()+", Value:" + record.value());
                logger.info("Partition: " + record.partition());
                logger.info("\n",record.value(),"\n");
            }
        }
    }
}
