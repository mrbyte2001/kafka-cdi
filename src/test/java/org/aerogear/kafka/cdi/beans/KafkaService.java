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
package org.aerogear.kafka.cdi.beans;

import org.aerogear.kafka.ExtendedKafkaProducer;
import org.aerogear.kafka.SimpleKafkaProducer;
import org.aerogear.kafka.cdi.ReceiveMessageFromInjectedServiceTest;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.annotation.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class KafkaService {

    Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Inject
    @Producer
    @KafkaConfig
    private SimpleKafkaProducer<Integer, String> producer;

    @Inject
    @Producer
    @KafkaConfig("producer_ext1")
    private ExtendedKafkaProducer<Integer, String> extendedKafkaProducer;

    @Inject
    @Producer
    @KafkaConfig("producer_ext2")
    private ExtendedKafkaProducer<Integer, String> extended2KafkaProducer;

    public SimpleKafkaProducer returnSimpleProducer() {
        return producer;
    }

    public ExtendedKafkaProducer returnExtendedProducer() {
        return extendedKafkaProducer;
    }

    public ExtendedKafkaProducer returnExtendedProducer2() { return extended2KafkaProducer;}

    public void sendMessage() {
        logger.info("sending message to the topic....");
        producer.send(ReceiveMessageFromInjectedServiceTest.SIMPLE_PRODUCER_TOPIC_NAME, "This is only a test");
    }

    public void sendMessageWithHeader() {
        logger.info("sending message with header to the topic....");
        Map<String, byte[]> headers = new HashMap<>();
        headers.put("header.key", "header.value".getBytes(Charset.forName("UTF-8")));
        extendedKafkaProducer.send(ReceiveMessageFromInjectedServiceTest.EXTENDED_PRODUCER_TOPIC_NAME,"This is only a second test", headers);
    }

    public void sendMessageWithHeaderAndKey() {
        logger.info("sending message with header and key to the topic....");
        Map<String, byte[]> headers = new HashMap<>();
        headers.put("header.key", "header.value".getBytes(Charset.forName("UTF-8")));
        extended2KafkaProducer.send(ReceiveMessageFromInjectedServiceTest.OTHER_EXTENDED_PRODUCER_TOPIC_NAME, 1, "This is only a third test", headers);

    }
}
