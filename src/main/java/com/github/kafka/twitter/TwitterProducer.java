package com.github.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Twitter authentication variables
    String consumerKey = "8GvcPHVylETEFvl8gRBcCkwxE";
    String consumerSecret = "gQvk2fk4GIZaRJc1dgi4qJ1kMsmjbGED18JhG9bReF3OBQSwk7";
    String token = "1086409737657348097-0BbxpfveQ7ycg1ZBfaC9uhnqd8DLae";
    String tokenSecret = "pGAM1LxVU7DzH0ybjRabqa4Chbg9tPqeVx01dYFmJyt7R";
    // bearer: AAAAAAAAAAAAAAAAAAAAABFUVgEAAAAAChJJSngpyR1r4QXRiR682UEFILM%3D7lUvJonoPsoNxEmtuq9WcLwtWLuDIi9rd6LkTVCOLop4Nvi428
    List<String> terms = Lists.newArrayList("onlyfans");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create a twitter client

        Client hosebirdClient = createTwitterClient(msgQueue);
        hosebirdClient.connect();

        //create a kafka producer

        KafkaProducer<String,String> producer = createKafkaProducer();

        // add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down client from twitter...");
            hosebirdClient.stop();
            logger.info("Closing producer...");
            producer.close();
            logger.info("Done...");
        }));

        //loop to send tweets to kafka..


        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_topic", null, '\n'+msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null)
                        {
                            logger.error("Something bad happened while producing...",e);
                        }
                        else{
                            logger.info("Producing new msg...");
                        }
                    }
                });
            }



        }
        logger.info("End of Application");
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);       // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;
// Attempts to establish a connection.

    }

    public KafkaProducer<String,String> createKafkaProducer()
    {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        // bootstrap server address

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create safe producer
        // if idempotence is true every request will have an id and kafka will
        // be able to trace request, so in situation like ack lost, it will not duplicate,
        // order will be preserverd
        // even tries is highest integer can be, it will die after 2 minutes of trying
        // acks = all, all brokers will send ack.

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // create high throughput producer
        // snappy compression for smaller data send
        // linger.ms 0 -> 20 to make msges wait for batching
        // 32kb batch is more optimal for most cases than 16kb batch

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }
}
