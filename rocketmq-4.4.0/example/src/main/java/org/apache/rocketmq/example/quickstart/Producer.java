/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        final String TOPIC = "test-topic";

        final AtomicInteger sendNum = new AtomicInteger(0);

        String ADDR = "localhost:9876";

        /*
         * Instantiate with a producer group name.
         */
        final DefaultMQProducer producer = new DefaultMQProducer("PID-test-topic");

        final int size = 8;
        producer.setMaxMessageSize(size);
        producer.setNamesrvAddr(ADDR);
        producer.setInstanceName("Producer");

        /*
         * Launch the instance.
         */
        producer.start();

        final byte[] body = new byte[size];

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(8, 16, 60, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(128));
        final Message msg = new Message(TOPIC /* Topic */,
            "TagA" /* Tag */,
            body /* Message body */
        );

        List<Message> lists = new ArrayList<>();
        lists.add(msg);
        lists.add(msg);
        try {
            SendResult send = producer.send(lists);
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        }

        msg.setKeys(Arrays.asList("AAA","BBB","CCC"));
        for (int i = 0; i < 1; ++i) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            //for (int i = 0; i < 100; ++i) {
                            //    int index = RandomUtils.nextInt(0, size);
                            //    body[index] = 1;
                            //    index = RandomUtils.nextInt(0, size);
                            //    body[index] = 0;
                            //}


                            SendResult sendResult = producer.send(msg);
                            System.out.println(sendResult);
                            //TimeUnit.MILLISECONDS.sleep(1);
                            int i = sendNum.incrementAndGet();
                            if(i%10000 == 0) {
                                System.out.println("sendNum:" + i);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
            threadPoolExecutor.execute(runnable);
        }

        threadPoolExecutor.awaitTermination(1000,TimeUnit.SECONDS);
        threadPoolExecutor.shutdown();

        producer.shutdown();
    }
}
