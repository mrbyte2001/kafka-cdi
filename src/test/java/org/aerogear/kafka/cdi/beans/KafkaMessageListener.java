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

import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.ForTopic;
import org.aerogear.kafka.cdi.beans.mock.MessageReceiver;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static org.aerogear.kafka.cdi.ReceiveMessageFromInjectedServiceTest.EXTENDED_PRODUCER_TOPIC_NAME;
import static org.aerogear.kafka.cdi.ReceiveMessageFromInjectedServiceTest.OTHER_EXTENDED_PRODUCER_TOPIC_NAME;
import static org.aerogear.kafka.cdi.ReceiveMessageFromInjectedServiceTest.SIMPLE_PRODUCER_TOPIC_NAME;

public class KafkaMessageListener {

    @Inject
    @ForTopic(SIMPLE_PRODUCER_TOPIC_NAME)
    private MessageReceiver simpleTopicReceiver;


    @Inject
    @ForTopic(EXTENDED_PRODUCER_TOPIC_NAME)
    private MessageReceiver extendedTopicReceiver;

    @Inject
    @ForTopic(OTHER_EXTENDED_PRODUCER_TOPIC_NAME)
    private MessageReceiver otherExtendedTopicReceiver;

    private final Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

    @PostConstruct
    public void setup() {
        logger.info("Bean is ready!");
    }

    @Consumer(
            topics = "#{SIMPLE_TOPIC_NAME}",
            groupId = "projection-group",
            consumerRebalanceListener = MyConsumerRebalanceListener.class
    )
    public void onMessage(final String simplePayload) {
        logger.info("Got message: {} ", simplePayload);
        simpleTopicReceiver.ack();
    }

    @Consumer(
            topics = "ServiceInjectionTest_annotation#topics",
            groupId = "#{GROUP_ID}",
            consumerRebalanceListener = MyConsumerRebalanceListener.class
    )
    public void onMessage(final String simplePayload, final Headers headers) {
        logger.info("Got message: {}||{} ", simplePayload, headers);
        extendedTopicReceiver.ack();
    }

    @Consumer(
            topics = OTHER_EXTENDED_PRODUCER_TOPIC_NAME,
            consumerRebalanceListener = MyConsumerRebalanceListener.class
    )
    public void onMessage(final Integer key, final String simplePayload, final Headers headers) {
        logger.info("Got message: {}||{}||{} ",key, simplePayload, headers);
        otherExtendedTopicReceiver.ack();
    }
}
