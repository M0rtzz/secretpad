/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.service.handler.vote;

import org.secretflow.secretpad.common.util.Base64Utils;
import org.secretflow.secretpad.common.util.JsonUtils;
import org.secretflow.secretpad.manager.integration.node.NodeManager;
import org.secretflow.secretpad.persistence.entity.ProjectApprovalConfigDO;
import org.secretflow.secretpad.persistence.entity.ProjectDO;
import org.secretflow.secretpad.persistence.entity.ProjectInstDO;
import org.secretflow.secretpad.persistence.entity.VoteRequestDO;
import org.secretflow.secretpad.persistence.repository.InstRepository;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.persistence.repository.ProjectApprovalConfigRepository;
import org.secretflow.secretpad.persistence.repository.ProjectInstRepository;
import org.secretflow.secretpad.persistence.repository.ProjectNodeRepository;
import org.secretflow.secretpad.persistence.repository.ProjectRepository;
import org.secretflow.secretpad.persistence.repository.VoteInviteRepository;
import org.secretflow.secretpad.persistence.repository.VoteRequestRepository;
import org.secretflow.secretpad.service.CertificateService;
import org.secretflow.secretpad.service.EnvService;
import org.secretflow.secretpad.service.InstService;
import org.secretflow.secretpad.service.impl.InstServiceImpl;
import org.secretflow.secretpad.service.model.approval.ProjectCallBackAction;
import org.secretflow.secretpad.service.model.approval.VoteRequestBody;
import org.secretflow.secretpad.service.model.approval.VoteRequestMessage;
import org.secretflow.secretpad.service.model.message.AbstractVoteTypeMessage;
import org.secretflow.secretpad.service.model.message.ProjectApprovalCustomizedMessage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.CacheManager;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProjectCreateMessageHandlerTest {
    @Mock
    private VoteInviteRepository voteInviteRepository;
    @Mock
    private VoteRequestRepository voteRequestRepository;
    @Mock
    private NodeRepository nodeRepository;
    @Mock
    private InstRepository instRepository;
    @Mock
    private EnvService envService;
    @Mock
    private ProjectApprovalConfigRepository projectApprovalConfigRepository;
    @Mock
    private ProjectRepository projectRepository;
    @Mock
    private CertificateService certificateService;
    @Mock
    private NodeManager nodeManager;
    @Mock
    private ProjectNodeRepository projectNodeRepository;
    @Mock
    private ProjectInstRepository projectInstRepository;
    @Mock
    private CacheManager cacheManager;
    @Mock
    private InstService instService;

    private ProjectCreateMessageHandler handler;
    private String originalInstId;

    @BeforeEach
    void setUp() {
        originalInstId = InstServiceImpl.INST_ID;
        InstServiceImpl.INST_ID = "alice";
        handler = new ProjectCreateMessageHandler(
                voteInviteRepository, voteRequestRepository, nodeRepository, instRepository,
                envService, projectApprovalConfigRepository, projectRepository,
                certificateService, nodeManager, projectNodeRepository, projectInstRepository,
                cacheManager, instService);
    }

    @AfterEach
    void tearDown() {
        InstServiceImpl.INST_ID = originalInstId;
    }

    @Test
    void rejectedProjectShouldBeDeletedLocally() {
        ProjectDO project = project("project-1", "测试项目");
        VoteRequestDO voteRequest = rejectedVote(project);
        when(projectRepository.findById("project-1")).thenReturn(Optional.of(project));

        handler.doCallBackRejected(voteRequest);

        verify(projectInstRepository).deleteByUpkProjectId("project-1");
        verify(projectNodeRepository).deleteByUpkProjectId("project-1");
        verify(projectRepository).delete(project);
        verify(voteRequestRepository).saveAndFlush(voteRequest);
    }

    @Test
    void messageListShouldUseVoteSnapshotAfterProjectDeletion() {
        ProjectDO project = project("project-1", "测试项目");
        VoteRequestDO voteRequest = rejectedVote(project);
        ProjectApprovalConfigDO config = ProjectApprovalConfigDO.builder()
                .voteID("vote-1")
                .projectId("project-1")
                .build();
        when(projectApprovalConfigRepository.findById("vote-1")).thenReturn(Optional.of(config));
        when(voteRequestRepository.findById("vote-1")).thenReturn(Optional.of(voteRequest));
        when(projectRepository.findById("project-1")).thenReturn(Optional.empty());

        AbstractVoteTypeMessage message = handler.getMessageListNecessaryInfo("vote-1");

        ProjectApprovalCustomizedMessage projectMessage = (ProjectApprovalCustomizedMessage) message;
        assertEquals("project-1", projectMessage.getProjectId());
        assertEquals("MPC", projectMessage.getComputeMode());
    }

    private VoteRequestDO rejectedVote(ProjectDO project) {
        ProjectCallBackAction callbackAction = new ProjectCallBackAction();
        callbackAction.setProjectDO(project);
        callbackAction.setProjectInstDOS(List.of(
                ProjectInstDO.builder().upk(new ProjectInstDO.UPK(project.getProjectId(), "alice")).build(),
                ProjectInstDO.builder().upk(new ProjectInstDO.UPK(project.getProjectId(), "bob")).build()));
        callbackAction.setProjectNodeDOS(List.of());
        VoteRequestBody body = VoteRequestBody.builder()
                .rejectedAction("PROJECT_CREATE," + JsonUtils.toJSONString(callbackAction))
                .build();
        VoteRequestMessage message = VoteRequestMessage.builder()
                .body(Base64Utils.encode(JsonUtils.toJSONString(body).getBytes(StandardCharsets.UTF_8)))
                .build();
        VoteRequestDO voteRequest = new VoteRequestDO();
        voteRequest.setVoteID("vote-1");
        voteRequest.setRequestMsg(JsonUtils.toJSONString(message));
        return voteRequest;
    }

    private ProjectDO project(String projectId, String name) {
        return ProjectDO.builder()
                .projectId(projectId)
                .name(name)
                .ownerId("alice")
                .computeMode("MPC")
                .computeFunc("DAG")
                .build();
    }
}
