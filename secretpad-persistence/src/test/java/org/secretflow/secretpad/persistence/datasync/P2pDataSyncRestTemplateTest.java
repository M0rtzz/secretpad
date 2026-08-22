/*
 * Copyright 2026 Ant Group Co., Ltd.
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
package org.secretflow.secretpad.persistence.datasync;

import org.secretflow.secretpad.persistence.datasync.buffer.DataSyncDataBufferTemplate;
import org.secretflow.secretpad.persistence.datasync.listener.EntityChangeListener;
import org.secretflow.secretpad.persistence.datasync.producer.p2p.P2pPaddingNodeServiceImpl;
import org.secretflow.secretpad.persistence.datasync.rest.p2p.P2pDataSyncRestService;
import org.secretflow.secretpad.persistence.datasync.rest.p2p.P2pDataSyncRestTemplate;
import org.secretflow.secretpad.persistence.entity.BaseAggregationRoot;
import org.secretflow.secretpad.persistence.entity.ProjectDO;
import org.secretflow.secretpad.persistence.model.DbChangeAction;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.TaskScheduler;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class P2pDataSyncRestTemplateTest {

    @Test
    void failedSendShouldKeepHeadEventAndScheduleSingleRetry() throws Exception {
        DataSyncDataBufferTemplate buffer = mock(DataSyncDataBufferTemplate.class);
        P2pDataSyncRestService restService = mock(P2pDataSyncRestService.class);
        P2pPaddingNodeServiceImpl paddingNodeService = mock(P2pPaddingNodeServiceImpl.class);
        TaskScheduler retryScheduler = mock(TaskScheduler.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event = event();

        when(buffer.size("bob-inst")).thenReturn(1);
        when(buffer.peek("bob-inst")).thenReturn(event);
        when(paddingNodeService.turnInstToRouteId("bob-inst")).thenReturn("bob-node");
        when(restService.sync(anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("HTTP 503"));
        doReturn(scheduledFuture).when(retryScheduler).schedule(any(Runnable.class), any(Instant.class));

        P2pDataSyncRestTemplate template = new P2pDataSyncRestTemplate();
        template.setDataSyncDataBufferTemplate(buffer);
        template.setP2pPaddingNodeService(paddingNodeService);
        template.setP2pDataSyncRestService(restService);
        template.setMeterRegistry(new SimpleMeterRegistry());
        template.setRetryScheduler(retryScheduler);
        template.setApplicationEventPublisher(eventPublisher);

        template.send("bob-inst");

        verify(buffer).peek("bob-inst");
        verify(buffer, never()).poll("bob-inst");
        verify(buffer, never()).commit("bob-inst", event);
        verify(restService, times(1)).sync(anyString(), anyString(), anyString());
        verify(retryScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));
    }

    private EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event() {
        ProjectDO project = ProjectDO.builder().projectId("project").name("project").build();
        EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event = new EntityChangeListener.DbChangeEvent<>();
        event.setAction(DbChangeAction.UPDATE.val);
        event.setDType(ProjectDO.class.getTypeName());
        event.setNodeIds(List.of("bob-inst"));
        event.setSource(project);
        return event;
    }
}
