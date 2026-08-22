/*
 * Copyright 2023 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.secretpad.persistence.datasync.job;

import org.secretflow.secretpad.persistence.datasync.event.P2pDataSyncSendEvent;
import org.secretflow.secretpad.persistence.datasync.rest.DataSyncRestTemplate;

import jakarta.annotation.Resource;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * data sync job
 *
 * @author yutu
 * @date 2023/12/11
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "secretpad.datasync", value = "p2p", matchIfMissing = false, havingValue = "true")
public class DataSyncJob implements ApplicationListener<P2pDataSyncSendEvent> {

    private static final Map<String, String> NODE_WORK_THREAD_NAME = new ConcurrentHashMap<>();
    @Lazy
    @Resource
    @Setter
    private DataSyncRestTemplate dataSyncRestTemplate;

    /**
     * Perform data synchronization tasks
     *
     * @param node name
     * @throws InterruptedException Thread Interrupt Exception
     */
    public void work(String node) throws InterruptedException {
        // get the name of the current thread
        String threadName = Thread.currentThread().getName();
        // If the target node is included in the collection of available nodes
        log.debug("{} start", threadName);
        // Send a data synchronization request to the destination node
        dataSyncRestTemplate.send(node);
    }

    /**
     * Handle an application event.
     *
     * @param event the event to respond to
     */
    @Async("dataSyncThreadPool")
    @Override
    public void onApplicationEvent(P2pDataSyncSendEvent event) {
        String node = event.getNode();
        String threadName = Thread.currentThread().getName();
        boolean acquired = false;
        try {
            log.debug("start data sync to {}", node);
            String currentWorker = NODE_WORK_THREAD_NAME.putIfAbsent(node, threadName);
            if (currentWorker != null) {
                log.info("{} is working now {}, skip it", node, currentWorker);
                return;
            }
            acquired = true;
            work(node);
        } catch (Exception e) {
            log.error("dataSyncJob work error", e);
        } finally {
            if (acquired) {
                NODE_WORK_THREAD_NAME.remove(node, threadName);
            }
        }
    }
}
