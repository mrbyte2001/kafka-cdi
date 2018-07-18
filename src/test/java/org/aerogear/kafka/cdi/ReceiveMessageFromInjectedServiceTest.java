/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.annotation.ForTopic;
import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.mock.MessageReceiver;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.configuration.CustomTestConfigProviderResolver;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.spi.ConfigProviderResolver;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


@RunWith(Arquillian.class)
public class ReceiveMessageFromInjectedServiceTest extends KafkaClusterTestBase {

    public static final String SIMPLE_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.SimpleProducer";
    public static final String EXTENDED_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.ExtendedProducer";
    public static final String OTHER_EXTENDED_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.OtherExtendedProducer";

    private final Logger logger = LoggerFactory.getLogger(ReceiveMessageFromInjectedServiceTest.class);

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(MockProvider.class.getPackage())
                .addAsServiceProvider(ConfigProviderResolver.class, CustomTestConfigProviderResolver.class);
    }

    @Inject
    private KafkaService service;

    @BeforeClass
    public static void createTopic() {
        try {
            kafkaCluster.createTopics(SIMPLE_PRODUCER_TOPIC_NAME,EXTENDED_PRODUCER_TOPIC_NAME,OTHER_EXTENDED_PRODUCER_TOPIC_NAME);
        } catch (Exception e) {
            // Ignore.
        }
    }

    @Test
    public void testSendAndReceive(@ForTopic(SIMPLE_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {

        final String consumerId = SIMPLE_PRODUCER_TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessage();

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(SIMPLE_PRODUCER_TOPIC_NAME));

        boolean loop = true;

        while(loop) {

            final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, String> record : records) {
                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value(), equalTo("This is only a test"));
                loop = false;
            }
        }

        Mockito.verify(receiver, Mockito.times(1)).ack();
    }

    @Test
    public void testSendAndReceiveWithHeader(@ForTopic(EXTENDED_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {
        final String consumerId = EXTENDED_PRODUCER_TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessageWithHeader();

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(EXTENDED_PRODUCER_TOPIC_NAME));

        boolean loop = true;

        while(loop) {

            final ConsumerRecords<Integer, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<Integer, String> record : records) {
                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value(), equalTo("This is only a second test"));
                final Headers headers = record.headers();
                final Header header = headers.lastHeader("header.key");
                assertThat(header.key(), equalTo("header.key"));
                loop = false;
            }
        }

        Mockito.verify(receiver, Mockito.times(1)).ack();
    }

    @Test
    public void testSendAndReceiveWithHeaderAndKey(@ForTopic(OTHER_EXTENDED_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {
        final String consumerId = OTHER_EXTENDED_PRODUCER_TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessageWithHeaderAndKey();

        Properties cconfig = kafkaCluster.useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(OTHER_EXTENDED_PRODUCER_TOPIC_NAME));

        boolean loop = true;

        while(loop) {

            final ConsumerRecords<Integer, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<Integer, String> record : records) {
                assertThat(record.key(), equalTo(1));

                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value(), equalTo("This is only a third test"));
                final Headers headers = record.headers();
                final Header header = headers.lastHeader("header.key");
                assertThat(header.key(), equalTo("header.key"));
                loop = false;
            }
        }

        Mockito.verify(receiver, Mockito.times(1)).ack();
    }
}
