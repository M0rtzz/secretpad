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

package org.secretflow.secretpad.kuscia.v1alpha1.mock.service;

import io.grpc.stub.StreamObserver;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Job;
import org.secretflow.v1alpha1.kusciaapi.JobServiceGrpc;

/**
 * @author yutu
 * @date 2024/06/19
 */
public class JobService extends JobServiceGrpc.JobServiceImplBase implements CommonService {

    /**
     * Test-only configurable state. Defaults keep the historical behaviour (all calls succeed);
     * integration tests set these fields before driving a scenario.
     */
    public static final class State {
        public static volatile int createJobCode = KusciaAPIConstants.OK;
        public static volatile String createJobMessage = "success";
        public static volatile int jobQueryCode = KusciaAPIConstants.OK;
        public static volatile String jobState = "RUNNING";
        public static volatile String taskState = "";
        public static volatile String partyState = "";
        public static volatile String jobErrMsg = "";
        public static volatile boolean withEndpoints = false;
        public static volatile String endpointPortName = "web";
        public static volatile String endpointScope = "Cluster";
        public static volatile String endpointAddress = "10.0.0.1:31234";
        public static volatile int stopJobCode = KusciaAPIConstants.OK;
        public static volatile String stopJobMessage = "success";
        public static volatile int deleteJobCode = KusciaAPIConstants.OK;
        public static volatile String deleteJobMessage = "success";
        /** 最近一次 createJob 请求原文（Z-02 网络隔离断言 -nonet 变体使用）。 */
        public static volatile Job.CreateJobRequest lastCreateJobRequest = null;
    }

    private Common.Status status(int code, String message) {
        return Common.Status.newBuilder().setCode(code).setMessage(message).build();
    }

    private Job.QueryJobResponseData queryData() {
        String taskState = State.taskState.isBlank() ? State.jobState : State.taskState;
        String partyState = State.partyState.isBlank() ? taskState : State.partyState;
        Job.TaskStatus.Builder task = Job.TaskStatus.newBuilder().setTaskId("data-sandbox-task").setState(taskState);
        Job.PartyStatus.Builder party = Job.PartyStatus.newBuilder().setDomainId("kuscia-system").setState(partyState).setErrMsg(State.jobErrMsg);
        if (State.withEndpoints) {
            party.addEndpoints(Job.JobPartyEndpoint.newBuilder()
                    .setPortName(State.endpointPortName).setScope(State.endpointScope).setEndpoint(State.endpointAddress));
        }
        Job.JobStatusDetail status = Job.JobStatusDetail.newBuilder().setState(State.jobState).setErrMsg(State.jobErrMsg)
                .addTasks(task.addParties(party)).build();
        return Job.QueryJobResponseData.newBuilder().setJobId("ds-test").setStatus(status).build();
    }

    @Override
    public void queryJob(Job.QueryJobRequest request, StreamObserver<Job.QueryJobResponse> responseObserver) {
        Job.QueryJobResponse resp;
        if (State.jobQueryCode == KusciaAPIConstants.OK) {
            resp = Job.QueryJobResponse.newBuilder().setStatus(getStatus()).setData(queryData()).build();
        } else {
            resp = Job.QueryJobResponse.newBuilder().setStatus(status(State.jobQueryCode, "job not found")).build();
        }
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void createJob(Job.CreateJobRequest request, StreamObserver<Job.CreateJobResponse> responseObserver) {
        State.lastCreateJobRequest = request;
        Job.CreateJobResponse resp = Job.CreateJobResponse.newBuilder().setStatus(status(State.createJobCode, State.createJobMessage)).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void batchQueryJobStatus(Job.BatchQueryJobStatusRequest request, StreamObserver<Job.BatchQueryJobStatusResponse> responseObserver) {
        Job.BatchQueryJobStatusResponse resp = Job.BatchQueryJobStatusResponse.newBuilder().setStatus(getStatus()).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void stopJob(Job.StopJobRequest request, StreamObserver<Job.StopJobResponse> responseObserver) {
        Job.StopJobResponse resp = Job.StopJobResponse.newBuilder().setStatus(status(State.stopJobCode, State.stopJobMessage)).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void deleteJob(Job.DeleteJobRequest request, StreamObserver<Job.DeleteJobResponse> responseObserver) {
        Job.DeleteJobResponse resp = Job.DeleteJobResponse.newBuilder().setStatus(status(State.deleteJobCode, State.deleteJobMessage)).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void restartJob(Job.RestartJobRequest request, StreamObserver<Job.RestartJobResponse> responseObserver) {
        Job.RestartJobResponse resp = Job.RestartJobResponse.newBuilder().setStatus(getStatus()).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelJob(Job.CancelJobRequest request, StreamObserver<Job.CancelJobResponse> responseObserver) {
        Job.CancelJobResponse resp = Job.CancelJobResponse.newBuilder().setStatus(getStatus()).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void approveJob(Job.ApproveJobRequest request, StreamObserver<Job.ApproveJobResponse> responseObserver) {
        Job.ApproveJobResponse resp = Job.ApproveJobResponse.newBuilder().setStatus(getStatus()).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void watchJob(Job.WatchJobRequest request, StreamObserver<Job.WatchJobEventResponse> responseObserver) {
        Job.WatchJobEventResponse resp = Job.WatchJobEventResponse.newBuilder().build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void suspendJob(Job.SuspendJobRequest request, StreamObserver<Job.SuspendJobResponse> responseObserver) {
        Job.SuspendJobResponse resp = Job.SuspendJobResponse.newBuilder().setStatus(getStatus()).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }
}
