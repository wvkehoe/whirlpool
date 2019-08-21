package com.khs.microservice.whirlpool.topicservice;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.khs.microservice.whirlpool.common.DataResponse;
import com.khs.microservice.whirlpool.common.Message;
import com.khs.microservice.whirlpool.common.MessageConstants;
import com.khs.microservice.whirlpool.httpclient.HttpClientHelper;
import com.khs.microservice.whirlpool.service.BaseService;
import io.netty.channel.Channel;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * This producer will send stock updates to the stock-ticker topic.
 */
public class TopicService extends BaseService {

    protected class SubscriptionSet {
        protected HashSet<String> mTopics;
        protected KafkaConsumer<String, String> mConsumer;
        protected int mTimeouts = 0;

        SubscriptionSet(String topic) throws Exception {
            mTopics = new HashSet<String>();
            mTopics.add(topic);
        }

        void Subscribe() throws Exception {
            assert(!mTopics.isEmpty());

            if(mConsumer == null) {
                try (InputStream props = Resources.getResource("user_topic_consumer.props").openStream()) {
                    Properties properties = new Properties();
                    properties.load(props);
                    mConsumer = new KafkaConsumer<>(properties);
                }
            }
            mConsumer.subscribe(new ArrayList<String>(Arrays.asList(mTopics.toArray(new String[0]))));
        }

        void Cancel() throws Exception {
            mConsumer.close();
            mConsumer = null;
        }

        Set<String> GetTopics() { return mTopics; }

        KafkaConsumer<String, String> GetConsumer() { return mConsumer; }
    }

    // private static final String STOCK_URL_START = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20yahoo.finance.quotes%20where%20symbol%20in%20(";
    // private static final String STOCK_URL_END = ")%0A%09%09&env=http%3A%2F%2Fdatatables.org%2Falltables.env&format=json";
    private final HashMap<String, SubscriptionSet> userSubscriptionSets;

    public static void main(String[] args) throws IOException {
        TopicService service = new TopicService();
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public TopicService() {
        super();
        userSubscriptionSets = new HashMap<String, SubscriptionSet>();
        startServer("topic-cmd", "topic", 1000L);
    }

    @Override
    protected String getCommandType() {
        return "TopicCommand";
    }

    @Override
    protected void addSubscription(String user, String topic) {
        // first see if we already have a subscription set for this user
        SubscriptionSet ss = userSubscriptionSets.get(user);
        if (ss == null) {
            try {
                ss = new SubscriptionSet(topic);
                userSubscriptionSets.put(user, ss);
                ss.Subscribe();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        } else {
            // add this new topic if it's not already subscribed to
            if (!ss.GetTopics().contains(topic)) {
                ss.GetTopics().add(topic);
                try {
                    ss.Subscribe();     // re-apply the subscription
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    protected void removeSubscription(String user, String topic) {
        // first verify that we already have subscription set for this user
        SubscriptionSet ss = userSubscriptionSets.get(user);
        if (ss != null) {
            Boolean hadSubscriptions = !ss.GetTopics().isEmpty();
            ss.GetTopics().remove(topic);
            if (hadSubscriptions && ss.GetTopics().isEmpty()) {
                try {
                    ss.Cancel();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    protected void cleanup() {
        // close all of the open consumers we have for users
        for(SubscriptionSet ss : userSubscriptionSets.values()) {
            try {
                ss.Cancel();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    protected void collectData(Gson gson, String user, List<String> topics) {
        Map<String, String> subscriptionData = new HashMap<>();

        SubscriptionSet ss = userSubscriptionSets.get(user);

        if(ss != null) {
            KafkaConsumer<String, String> consumer = ss.GetConsumer();
            if(consumer != null) {
                ConsumerRecords<String, String> records = consumer.poll(200);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        subscriptionData.put(record.topic(), record.value());
                        DataResponse response = new DataResponse();
                        response.setType("TopicResponse");
                        response.setId(user);
                        response.setResult(MessageConstants.SUCCESS);
                        response.setSubscriptionData(subscriptionData);
                        responseQueue.add(gson.toJson(response));
                    }
                }
            }
        }
    }
}