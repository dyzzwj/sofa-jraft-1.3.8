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
package com.alipay.sofa.jraft.example.counter;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.BytesUtil;

/**
 * @author likun (saimu.msm@antfin.com)
 */
public class CounterServiceImpl implements CounterService {
    private static final Logger LOG = LoggerFactory.getLogger(CounterServiceImpl.class);

    private final CounterServer counterServer;
    private final Executor      readIndexExecutor;

    public CounterServiceImpl(CounterServer counterServer) {
        this.counterServer = counterServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    @Override
    public void get(final boolean readOnlySafe, final CounterClosure closure) {
        // readOnlySafe = false，不走一致性读逻辑，直接返回当前节点的statemachine中的值
        if(!readOnlySafe){
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }
        // readOnlySafe = true，走一致性读逻辑

        /**
         * 针对一致性读，有ReadIndex和LeaseRead两种可选方案，默认使用ReadIndex方案。
         *  1、ReadIndex（ReadOnlySafe）：需要向其他Follower发送心跳，确认当前节点任然是leader；
         *  2、LeaseRead（ReadOnlyLeaseBased）：为了减少发送心跳rpc请求的次数，每次leader向follower发送心跳，会更新一个时间戳，如果这个读请求在心跳超时时间之内，可以认为当前节点仍然是leader。

         */
        //当readIndex执行完成后，执行用户的ReadIndexClosure回调。
        //com.alipay.sofa.jraft.core.NodeImpl.readIndex
        this.counterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                // 保证readIndex(commitIndex) <= applyIndex后，获取状态机中的值
                if(status.isOk()){
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                // 失败处理
                CounterServiceImpl.this.readIndexExecutor.execute(() -> {
                    if(isLeader()){
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        // 如果当前节点是Leader，提交task到Raft集群，如果成功了，会回调CounterStateMachine的onApply方法响应
                        applyOperation(CounterOperation.createGet(), closure);
                    }else {
                        // 如果当前节点不是Leader，响应失败
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    private boolean isLeader() {
        return this.counterServer.getFsm().isLeader();
    }

    // 获取CounterStateMachine中的value
    private long getValue() {
        return this.counterServer.getFsm().getValue();
    }

    private String getRedirect() {
        return this.counterServer.redirect().getRedirect();
    }

    @Override
    public void incrementAndGet(final long delta, final CounterClosure closure) {
        // 将业务请求delta转换为CounterOperation
        applyOperation(CounterOperation.createIncrement(delta), closure);
    }

    private void applyOperation(final CounterOperation op, final CounterClosure closure) {
        if (!isLeader()) {
            // 响应请求失败，应该将请求转发至Leader节点
            handlerNotLeaderError(closure);
            return;
        }

        try {
            //设置请求入参到closure里
            closure.setCounterOperation(op);
            // 创建Task
            final Task task = new Task();
            // 把请求入参序列化为ByteBuffer
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            // 把外部传入的closure放到Task的done成员变量里
            //task的done(closure)在CounterStateMachine.onApply中由用户手动调用
            task.setDone(closure);
            // 将Task提交到当前Node处理，托管给JRaft框架
            this.counterServer.getNode().apply(task);
            /**
             * 执行完Node.apply(Task)后，后续的步骤会托管给JRaft框架处理。
             * Leader会将Log同步给follower节点，当n/2+1节点同步成功后，会触发StateMachine(CounterStateMachine)的onApply方法，继续走用户的业务逻辑。
             */


        } catch (CodecException e) {
            // 如果发生编解码异常，这里直接响应客户端
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final CounterClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
}
