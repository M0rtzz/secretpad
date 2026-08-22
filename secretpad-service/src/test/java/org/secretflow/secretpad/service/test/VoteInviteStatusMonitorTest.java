/*
 * Copyright 2024 Ant Group Co., Ltd.
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

package org.secretflow.secretpad.service.test;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.persistence.entity.VoteInviteDO;
import org.secretflow.secretpad.persistence.entity.VoteRequestDO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.persistence.repository.VoteInviteRepository;
import org.secretflow.secretpad.persistence.repository.VoteRequestRepository;
import org.secretflow.secretpad.service.EnvService;
import org.secretflow.secretpad.service.enums.VoteStatusEnum;
import org.secretflow.secretpad.service.enums.VoteTypeEnum;
import org.secretflow.secretpad.service.schedule.VoteInviteStatusMonitor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;

/**
 * @author yutu
 * @date 2024/07/05
 */
@ExtendWith(MockitoExtension.class)
public class VoteInviteStatusMonitorTest {
    @Spy
    VoteInviteStatusMonitor voteInviteStatusMonitor;

    @Mock
    private VoteRequestRepository voteRequestRepository;
    @Mock
    private VoteInviteRepository voteInviteRepository;
    @Mock
    private EnvService envService;

    @Mock
    private NodeRepository nodeRepository;

    @BeforeEach
    public void setup() {
        UserContextDTO userContextDTO = UserContextDTO.builder()
                .ownerId("alice")
                .platformType(PlatformTypeEnum.AUTONOMY)
                .build();
        UserContext.setBaseUser(userContextDTO);
    }

    @AfterEach
    void tearDown() {
        UserContext.remove();
    }

    @Test
    void syncShouldConvergeApprovedInviteToVoteRequest() {
        VoteRequestDO voteRequestDO = voteRequest(VoteStatusEnum.REVIEWING.name());
        VoteInviteDO voteInviteDO = voteInvite(VoteStatusEnum.APPROVED.name(), "approved");
        arrange(voteRequestDO, voteInviteDO);

        voteInviteStatusMonitor.sync();

        assertEquals(VoteStatusEnum.APPROVED.getCode(), voteRequestDO.getStatus());
        assertPartyStatus(voteRequestDO, "bob", VoteStatusEnum.APPROVED.name(), "approved");
        assertPartyStatus(voteRequestDO, "alice", VoteStatusEnum.APPROVED.name(), null);
        assertEquals(2, voteRequestDO.getPartyVoteInfos().size());
        verify(voteRequestRepository).save(voteRequestDO);
    }

    @Test
    void syncShouldConvergeRejectedInviteToVoteRequest() {
        VoteRequestDO voteRequestDO = voteRequest(VoteStatusEnum.REVIEWING.name());
        VoteInviteDO voteInviteDO = voteInvite(VoteStatusEnum.REJECTED.name(), "rejected");
        arrange(voteRequestDO, voteInviteDO);

        voteInviteStatusMonitor.sync();

        assertEquals(VoteStatusEnum.REJECTED.getCode(), voteRequestDO.getStatus());
        assertPartyStatus(voteRequestDO, "bob", VoteStatusEnum.REJECTED.name(), "rejected");
        assertEquals(2, voteRequestDO.getPartyVoteInfos().size());
        verify(voteRequestRepository).save(voteRequestDO);
    }

    private void arrange(VoteRequestDO voteRequestDO, VoteInviteDO voteInviteDO) {
        voteInviteStatusMonitor.setVoteRequestRepository(voteRequestRepository);
        voteInviteStatusMonitor.setVoteInviteRepository(voteInviteRepository);
        voteInviteStatusMonitor.setEnvService(envService);
        voteInviteStatusMonitor.setNodeRepository(nodeRepository);
        Mockito.when(voteRequestRepository.findByStatus(anyInt())).thenReturn(List.of(voteRequestDO));
        Mockito.when(voteInviteRepository.findByVoteID(anyString())).thenReturn(List.of(voteInviteDO));
        Mockito.when(nodeRepository.findByInstId("alice")).thenReturn(List.of());
    }

    private VoteRequestDO voteRequest(String inviteeAction) {
        VoteRequestDO voteRequestDO = new VoteRequestDO();
        voteRequestDO.setType(VoteTypeEnum.PROJECT_CREATE.name());
        voteRequestDO.setExecutors(List.of());
        voteRequestDO.setVoteID("vote");
        voteRequestDO.setInitiator("alice");
        voteRequestDO.setStatus(VoteStatusEnum.REVIEWING.getCode());
        HashSet<VoteRequestDO.PartyVoteInfo> partyVoteInfos = new HashSet<>(Set.of(
                VoteRequestDO.PartyVoteInfo.builder().action(VoteStatusEnum.APPROVED.name()).partyId("alice").build(),
                VoteRequestDO.PartyVoteInfo.builder().action(inviteeAction).partyId("bob").build()));
        voteRequestDO.setPartyVoteInfos(partyVoteInfos);
        return voteRequestDO;
    }

    private VoteInviteDO voteInvite(String action, String reason) {
        VoteInviteDO voteInviteDO = new VoteInviteDO();
        voteInviteDO.setUpk(new VoteInviteDO.UPK("vote", "bob"));
        voteInviteDO.setAction(action);
        voteInviteDO.setReason(reason);
        return voteInviteDO;
    }

    private void assertPartyStatus(VoteRequestDO voteRequestDO, String partyId, String action, String reason) {
        VoteRequestDO.PartyVoteInfo partyVoteInfo = voteRequestDO.getPartyVoteInfos().stream()
                .filter(item -> partyId.equals(item.getPartyId()))
                .findFirst()
                .orElseThrow();
        assertEquals(action, partyVoteInfo.getAction());
        assertEquals(reason, partyVoteInfo.getReason());
    }
}
