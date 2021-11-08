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

import static com.alipay.sofa.jraft.example.counter.CounterOperation.GET;
import static com.alipay.sofa.jraft.example.counter.CounterOperation.INCREMENT;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.example.counter.snapshot.CounterSnapshotFile;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Counter state machine.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 4:52:31 PM
 */
public class CounterStateMachine extends StateMachineAdapter {

    private static final Logger LOG        = LoggerFactory.getLogger(CounterStateMachine.class);

    /**
     * Counter value
     *  原子计数器
     */
    private final AtomicLong    value      = new AtomicLong(0);
    /**
     * Leader term
     */
    private final AtomicLong    leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    /**
     * Returns current value.
     */
    public long getValue() {
        return this.value.get();
    }


    /**
     * 。当Node提交Task，对应的log被提交到Raft集群后，当quorum节点成功commit log，触发这个方法来应用状态（当前节点存储数据）。
     * CounterStateMachine在onApply方法中，执行原子计数器的相关功能，包括get和addAndGet。
     *
     * @param iter iterator of states
     */
    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            long current = 0;
            CounterOperation counterOperation = null;

            CounterClosure closure = null;
            // iter.done() != null代表当前节点是leader
            // 可以通过closure直接拿到请求数据，不需要反序列化请求报文
            if (iter.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                closure = (CounterClosure) iter.done();
                counterOperation = closure.getCounterOperation();
            } else {
                // iter.done == null 代表当前节点是follower，需要反序列化请求报文
                // Have to parse FetchAddRequest from this user log.
                final ByteBuffer data = iter.getData();
                try {
                    counterOperation = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(
                        data.array(), CounterOperation.class.getName());
                } catch (final CodecException e) {
                    LOG.error("Fail to decode IncrementAndGetRequest", e);
                }
            }
            // 执行业务逻辑
            if (counterOperation != null) {
                switch (counterOperation.getOp()) {
                    case GET:
                        // get
                        current = this.value.get();
                        LOG.info("Get value={} at logIndex={}", current, iter.getIndex());
                        break;
                    case INCREMENT:
                        // addAndGet
                        final long delta = counterOperation.getDelta();
                        final long prev = this.value.get();
                        current = this.value.addAndGet(delta);
                        LOG.info("Added value={} by delta={} at logIndex={}", prev, delta, iter.getIndex());
                        break;
                }

                // 如果closure不为空，代表当前节点是leader，需要调用closure.run来通知客户端
                if (closure != null) {
                    closure.success(current);
                    closure.run(Status.OK());
                }
            }
            iter.next();
        }
    }

    /**
     * // CounterStateMachine将内存中的计数写入磁盘
     */
    @Override
  public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
    final long currVal = this.value.get();
    Utils.runInThread(() -> {
      final CounterSnapshotFile snapshot = new CounterSnapshotFile(writer.getPath() + File.separator + "data");
      if (snapshot.save(currVal)) {
        if (writer.addFile("data")) {
          done.run(Status.OK());
        } else {
          done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
        }
      } else {
        done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
      }
    });
  }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    /**
     * // CounterStateMachine加载快照到内存计数器
     * @param reader
     * @return
     */
    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final CounterSnapshotFile snapshot = new CounterSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            this.value.set(snapshot.load());
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}
