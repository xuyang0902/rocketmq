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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 *
 *
 * 妈呀，这算法如果cidAll一直在变 怎么办呀
 *
 * 简单的意思就是给当前客户端分配指定的消息队列
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(

            String consumerGroup/*消费组*/,
            String currentCID/*当前client的id*/,
            List<MessageQueue> mqAll/*broker返回的消息队列列表*/,
            List<String> cidAll/*所有clientId集合*/
    ) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //找到自己是第几个客户端
        int index = cidAll.indexOf(currentCID);
        //队列总数%客户端总数取模
        int mod = mqAll.size() % cidAll.size();

        /**
         * 算一下平均每个客户端能承受多少个？
         *
         * 如果客户端比消费队列个数还要多，一个客户端负责一个队列
         *
         * -->mod>0 废代码，，，size不会为负数，只要mqAll和cidAll个数不一样 mod肯定大于0
         * -->后面的意思就是如果 index在mod前面，这台机器是需要负责多一台的，否则的话，这台机器只需要负责平均数的那台就可以了
         */
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());

        /**
         * 在mode前面的 index* 平均机器
         * 在mode后面的 index* 平均机器 + mode？？？？
         *
         * 意思应该就是 排在当前机器前面 一共有多少queue已经被其他消费端负责了的意思
         */
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        //剩下的queue大小和平均数求最小值
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
