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

package org.secretflow.secretpad.persistence.datasync;


import org.secretflow.secretpad.persistence.datasync.listener.EntityChangeListener;
import org.secretflow.secretpad.persistence.datasync.producer.p2p.P2pPaddingNodeServiceImpl;
import org.secretflow.secretpad.persistence.entity.BaseAggregationRoot;
import org.secretflow.secretpad.persistence.entity.VoteInviteDO;
import org.secretflow.secretpad.persistence.entity.VoteRequestDO;
import org.secretflow.secretpad.persistence.model.NodeInstDTO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.persistence.repository.VoteRequestRepository;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author yutu
 * @date 2024/07/05
 */
@ExtendWith(MockitoExtension.class)
public class P2pPaddingNodeServiceImplTest {

    @Mock
    private VoteRequestRepository voteRequestRepository;

    @Mock
    private NodeRepository nodeRepository;


    @Test
    public void testCompensate_NotVoteInviteDO() {
        P2pPaddingNodeServiceImpl p2pPaddingNodeServiceImpl = new P2pPaddingNodeServiceImpl(null, null, voteRequestRepository, null, null);
        VoteInviteDO source = new VoteInviteDO();
        source.setUpk(VoteInviteDO.UPK.builder().voteID("1").votePartitionID("alice").build());

        EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event = new EntityChangeListener.DbChangeEvent<>();
        event.setSource(source);
        VoteRequestDO build = VoteRequestDO.builder()
                .partyVoteInfos(Set.of(VoteRequestDO.PartyVoteInfo.builder().partyId("alice").build()))
                .build();
        Optional<VoteRequestDO> voteRequestDOOptional = Optional.of(build);
        Mockito.when(voteRequestRepository.findById(Mockito.any())).thenReturn(voteRequestDOOptional);
        Mockito.when(voteRequestRepository.save(Mockito.any())).thenReturn(null);
        p2pPaddingNodeServiceImpl.compensate(event);
    }

    @Test
    public void shouldResolvePersistedRouteImmediatelyAfterRestart() {
        NodeInstDTO route = Mockito.mock(NodeInstDTO.class);
        Mockito.when(route.getInstId()).thenReturn("partner-inst");
        Mockito.when(route.getMasterNodeId()).thenReturn("partner-master-node");
        Mockito.when(nodeRepository.findInstMasterNodeId()).thenReturn(List.of(route));
        P2pPaddingNodeServiceImpl service = new P2pPaddingNodeServiceImpl(null, null, null, null, nodeRepository);

        service.initializeRouteMappings();

        assertEquals("partner-master-node", service.turnInstToRouteId("partner-inst"));
    }
}
