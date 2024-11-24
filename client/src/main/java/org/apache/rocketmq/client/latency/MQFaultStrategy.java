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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

// 消息失败策略，延迟实现的门面类
public class MQFaultStrategy {
    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);
    // 记录有问题的 brokerName
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 判断是否启用 broker 故障延迟机制

        // 如果启用了
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                // 遍历 topic 下的所有消息队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = index++ % tpInfo.getMessageQueueList().size();
                    // 结合 tpInfo 内部的 index 选择一个消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 如果该消息队列的 brokerName 可用，则直接返回该 mq
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                // 否则，说明每一个 brokerName 都不可用，此时从 latencyFaultTolerance 中选择一个 brokerName
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 确定该 brokerName 对应的 write 消息队列数量
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 如果 writeQueueNums 为 0，则将该 brokerName 从 latencyFaultTolerance 中移除
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            // 轮询选择一个消息队列
            return tpInfo.selectOneMessageQueue();
        }

        // 如果未启用 broker 故障延迟机制，则直接选择一个消息队列；
        // 会尽量选择一个与 lastBrokerName 不同的消息队列
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            // 计算该 broker 的不可用持续时间
            // 如果 isolation 为 true，使用 30s 作为不可用持续时间；否则使用 currentLatency 作为不可用持续时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 更新并记录该 broker 的 FaultItem
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    // 计算不可用的持续时间
    private long computeNotAvailableDuration(final long currentLatency) {
        // 根据 currentLatency 本次消息发送的延迟时间，从 latencyMax 尾部向前找到第一个比 currentLatency 小的值
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                // 返回相同 index 位置的 notAvailableDuration
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
