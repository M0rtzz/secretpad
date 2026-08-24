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

package org.secretflow.secretpad.persistence.datasync.producer.p2p;

import org.secretflow.secretpad.common.constant.CacheConstants;
import org.secretflow.secretpad.persistence.datasync.listener.EntityChangeListener;
import org.secretflow.secretpad.persistence.datasync.producer.PaddingNodeService;
import org.secretflow.secretpad.persistence.entity.*;
import org.secretflow.secretpad.persistence.model.NodeInstDTO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.persistence.repository.ProjectApprovalConfigRepository;
import org.secretflow.secretpad.persistence.repository.ProjectInstRepository;
import org.secretflow.secretpad.persistence.repository.VoteRequestRepository;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author yutu
 * @date 2023/12/14
 */
@Slf4j
@RequiredArgsConstructor
public class P2pPaddingNodeServiceImpl implements PaddingNodeService {

    private final ProjectInstRepository projectInstRepository;

    private final ProjectApprovalConfigRepository projectApprovalConfigRepository;

    private final VoteRequestRepository voteRequestRepository;

    private final CacheManager cacheManager;

    private final NodeRepository nodeRepository;

    private Map<String, String> inst_Node = new ConcurrentHashMap<>();

    @PostConstruct
    public void initializeRouteMappings() {
        refreshRouteMappings();
    }

    @Override
    public void paddingNodes(EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event) {
        String projectId = event.getProjectId();
        List<String> nodeIds = new ArrayList<>();
        if (event.getSource() instanceof VoteRequestDO || event.getSource() instanceof VoteInviteDO) {
            nodeIds = event.getNodeIds();
        }
        if (StringUtils.isNotEmpty(projectId)) {
            List<ProjectInstDO> pis = projectInstRepository.findByUpkProjectId(projectId);
            if (!CollectionUtils.isEmpty(pis)) {
                for (ProjectInstDO p : pis) {
                    nodeIds.add(p.getUpk().getInstId());
                }
            }

            Optional<ProjectApprovalConfigDO> projectApprovalConfigDOOptional = projectApprovalConfigRepository.findByProjectIdAndType(projectId, "PROJECT_CREATE");
            if (projectApprovalConfigDOOptional.isPresent()) {
                nodeIds.addAll(projectApprovalConfigDOOptional.get().getNodeIds());
            } else {
                Cache cache = Objects.requireNonNull(cacheManager.getCache(CacheConstants.PROJECT_VOTE_PARTIES_CACHE));
                if (Objects.nonNull(cache.get(projectId))) {
                    ArrayList<String> parties = (ArrayList) cache.get(projectId).get();
                    if (!CollectionUtils.isEmpty(parties)) {
                        log.info("cache hit,projectId ={}, parties ={}", projectId, parties);
                        nodeIds.addAll(parties);
                    }
                }
            }
        }
        // Vote records use node IDs as their voter/partition identifiers so the
        // receiving node can query its inbox by platform node ID.  P2P routing,
        // however, is addressed by institution ID (which is mapped to the
        // institution's master node route below).  Translate only node IDs here
        // and retain institution IDs that are already present (for example the
        // vote initiator).
        if (event.getSource() instanceof VoteRequestDO || event.getSource() instanceof VoteInviteDO) {
            nodeIds = nodeIds.stream().map(this::toInstitutionRouteId).collect(Collectors.toList());
        }
        List<String> collect = nodeIds.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        event.setNodeIds(collect);
        refreshRouteMappings();
    }

    private String toInstitutionRouteId(String id) {
        if (StringUtils.isBlank(id)) {
            return id;
        }
        NodeDO node = nodeRepository.findByNodeId(id);
        return node == null || StringUtils.isBlank(node.getInstId()) ? id : node.getInstId();
    }

    @Override
    public void compensate(EntityChangeListener.DbChangeEvent<BaseAggregationRoot> event) {
        BaseAggregationRoot source = event.getSource();
        if (source instanceof VoteInviteDO) {
            VoteInviteDO voteInviteDO = (VoteInviteDO) event.getSource();
            Optional<VoteRequestDO> voteRequestDOOptional = voteRequestRepository.findById(voteInviteDO.getUpk().getVoteID());
            if (voteRequestDOOptional.isPresent()) {
                VoteRequestDO voteRequestDO = voteRequestDOOptional.get();
                Set<VoteRequestDO.PartyVoteInfo> partyVoteInfos = voteRequestDO.getPartyVoteInfos();
                for (VoteRequestDO.PartyVoteInfo partyVoteInfo : partyVoteInfos) {
                    if (voteInviteDO.getUpk().getVotePartitionID().equals(partyVoteInfo.getPartyId())) {
                        log.debug("inst -> {} compensate action {}", partyVoteInfo.getPartyId(), voteInviteDO.getAction());
                        partyVoteInfo.setAction(voteInviteDO.getAction());
                        partyVoteInfo.setReason(voteInviteDO.getReason());
                        voteRequestDO.setGmtModified(LocalDateTime.now());
                        voteRequestRepository.save(voteRequestDO);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void supInstInfo(AccountsDO accountsDO) {
        Assert.notNull(accountsDO, "accountsDO is null");
        Assert.notNull(accountsDO.getInstId(), "instId IS null");
        P2pDataSyncProducerTemplate.instId = accountsDO.getInstId();
    }

    @Override
    public String turnInstToRouteId(String instId) {
        String routeId = inst_Node.get(instId);
        if (StringUtils.isBlank(routeId)) {
            refreshRouteMappings();
            routeId = inst_Node.get(instId);
        }
        if (StringUtils.isBlank(routeId)) {
            log.warn("P2P route is missing for institution {}, available routes {}", instId, inst_Node.keySet());
        } else {
            log.info("P2P route resolved from institution {} to node {}", instId, routeId);
        }
        return routeId;
    }

    private void refreshRouteMappings() {
        List<NodeInstDTO> nodeDOList = nodeRepository.findInstMasterNodeId();
        for (NodeInstDTO nodeInstDto : nodeDOList) {
            if (StringUtils.isNotBlank(nodeInstDto.getInstId())
                    && StringUtils.isNotBlank(nodeInstDto.getMasterNodeId())) {
                inst_Node.put(nodeInstDto.getInstId(), nodeInstDto.getMasterNodeId());
            }
        }
    }
}
