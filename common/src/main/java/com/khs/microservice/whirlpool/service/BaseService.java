package com.khs.microservice.whirlpool.service;

import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.khs.microservice.whirlpool.common.Command;
import com.khs.microservice.whirlpool.common.CommandResponse;
import com.khs.microservice.whirlpool.common.MessageConstants;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains the common code for all the services
 */
public abstract class BaseService {
    protected ExecutorService consumerExecutor;
    protected ExecutorService producerExecutor;
    protected ExecutorService dataExecutor;
    protected final ConcurrentLinkedQueue<String> responseQueue = new ConcurrentLinkedQueue<>();
    protected final AtomicBoolean keepRunning = new AtomicBoolean(true);

    // Keep track of the subscriptions each user has asked for info about
    protected static Map<String, List<String>> allSubscriptions = new ConcurrentHashMap<>();

    public BaseService() {
    }

    protected abstract String getCommandType();

    protected abstract void collectData(Gson gson, String user, List<String> subscriptions);

    public void startServer(String commandTopic, String producerTopic) {
        consumerExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("consumer-%d")
                        .build()
        );

        producerExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("producer-%d")
                        .build()
        );

        dataExecutor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("producer-%d")
                        .build()
        );

        FutureTask<String> sendTickers = new FutureTask<>(new SendDataCallable(producerTopic));
        producerExecutor.execute(sendTickers);

        FutureTask<String> readTickers = new FutureTask<>(new ReaderCallable(commandTopic));
        consumerExecutor.execute(readTickers);

        FutureTask<String> dataTickers = new FutureTask<>(new DataCollectorCallable());
        dataExecutor.execute(dataTickers);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            keepRunning.set(false);
        }));
    }

    public class ReaderCallable implements Callable<String> {
        // one per callable as it is stateless, but not thread safe
        private Gson gson = new Gson();
        private String topic;

        public ReaderCallable(String topic) {
            this.topic = topic;
        }

        @Override
        public String call() throws Exception {
            // and the consumer
            KafkaConsumer<String, String> consumer;
            try (InputStream props = Resources.getResource("consumer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                consumer = new KafkaConsumer<>(properties);
            }

            consumer.subscribe(Collections.singletonList(topic));
            int timeouts = 0;

            try {
                while (keepRunning.get()) {
                    // read records with a short timeout. If we time out, we don't really care.
                    ConsumerRecords<String, String> records = consumer.poll(200);
                    if (records.count() == 0) {
                        timeouts++;
                    } else {
                        System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                        timeouts = 0;
                    }

                    CommandResponse response = new CommandResponse();
                    response.setType(getCommandType());

                    for (ConsumerRecord<String, String> record : records) {
                        if (record.topic().equals(topic)) {
                            List<String> items;
                            Command command = gson.fromJson(record.value(), Command.class);
                            response.setCommand(command.getCommand());
                            response.setSubscription(command.getSubscription());
                            response.setId(command.getId());
                            response.setErrorMessage(null);

                            if (command.getCommand() != null) {
                                if ("add".equals(command.getCommand())) {
                                    items = allSubscriptions.get(command.getId());
                                    if (items == null) {
                                        items = new CopyOnWriteArrayList<>();
                                    }

                                    items.add(command.getSubscription());
                                    allSubscriptions.put(command.getId(), items);
                                    response.setResult(MessageConstants.SUCCESS);
                                } else if ("remove".equals(command.getCommand())) {
                                    items = allSubscriptions.get(command.getId());

                                    if (items.contains(command.getSubscription())) {
                                        items.remove(command.getSubscription());
                                        allSubscriptions.put(command.getId(), items);
                                        response.setResult(MessageConstants.SUCCESS);
                                    } else {
                                        response.setResult(MessageConstants.FAILURE);
                                        response.setCommand(command.getCommand());
                                        response.setErrorMessage("Subscription: (" + command.getSubscription() + ") was not found");
                                    }
                                } else {
                                    response.setResult(MessageConstants.FAILURE);
                                    response.setErrorMessage("Command not recognized. " + record.value());
                                }
                            } else {
                                response.setResult(MessageConstants.FAILURE);
                                response.setErrorMessage("Command not recognized. " + record.value());
                            }

                            responseQueue.add(gson.toJson(response));
                        } else {
                            throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
                        }
                    }
                }
            } finally {
                consumer.close();
            }

            return "done";
        }
    }

    /**
     * This class runs as a thread. It periodically collects prices for ticker symbols
     * and places them on the responseQueue.
     *
     * @author jwb
     */
    public class DataCollectorCallable implements Callable<String> {
        // one per callable as it is stateless, but not thread safe
        private Gson gson = new Gson();

        public DataCollectorCallable() {
        }

        @Override
        public String call() throws Exception {
            try {
                while(keepRunning.get()) {
                    if (!allSubscriptions.isEmpty()) {
                        for (String user : allSubscriptions.keySet()) {
                            List<String> subscriptions = allSubscriptions.get(user);
                            if (!subscriptions.isEmpty()) {
                                collectData(gson, user, subscriptions);
                            }
                        }
                    }

                    // only try to send back to client every 2 seconds so it isn't overwhelmed with messages
                    Thread.sleep(10000L);
                }
            } catch (Throwable throwable) {
                System.out.printf("%s", throwable.getStackTrace());
            }

            return "done";
        }
    }

    /**
     * This class runs as a thread. It periodically collects weather data for city,state combos
     * and sends them to the weather topic. It also sends responses from the
     * command topic.
     *
     * @author jwb
     */
    class SendDataCallable implements Callable<String> {
        private String topic;

        public SendDataCallable(String topic) {
            this.topic = topic;
        }

        @Override
        public String call() throws Exception {
            // set up the producer
            KafkaProducer<String, String> producer;
            try (InputStream props = Resources.getResource("producer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                producer = new KafkaProducer<>(properties);
            }

            String message;

            try {
                while (keepRunning.get()) {
                    while ((message = responseQueue.poll()) != null) {
                        System.out.println("Sending message: " + message + " to topic: " + topic);
                        producer.send(new ProducerRecord<>(topic, message),
                                (metadata, e) -> {
                                    if (e != null) {
                                        e.printStackTrace();
                                    }

                                    System.out.println("The offset of the record we just sent is: " + metadata.offset());
                                });
                    }

                    producer.flush();

                    // only try to send back to client every 30 seconds so it isn't overwhelmed with messages
                    Thread.sleep(10L);
                }
            } catch (Throwable throwable) {
                System.out.printf("%s", throwable.getStackTrace());
            } finally {
                producer.close();
            }

            return "done";
        }
    }
}
