/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Client单例管理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class MQClientManager {
    private static MQClientManager instance = new MQClientManager();

    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    private ConcurrentHashMap<String/* clientId */, MQClientFactory> factoryTable =
            new ConcurrentHashMap<String, MQClientFactory>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }


    public MQClientFactory getAndCreateMQClientFactory(final ClientConfig clientConfig) {
        String clientId = clientConfig.buildMQClientId();
        MQClientFactory factory = this.factoryTable.get(clientId);
        if (null == factory) {
            factory =
                    new MQClientFactory(clientConfig.cloneClientConfig(),
                            this.factoryIndexGenerator.getAndIncrement(), clientId);
            MQClientFactory prev = this.factoryTable.putIfAbsent(clientId, factory);
            if (prev != null) {
                factory = prev;
            } else {
                // TODO log
            }
        }

        return factory;
    }


    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
