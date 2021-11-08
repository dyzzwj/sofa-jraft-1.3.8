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
package com.alipay.sofa.jraft.example.counter.rpc;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.example.counter.CounterClosure;
import com.alipay.sofa.jraft.example.counter.CounterService;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

/**
 * IncrementAndGetRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:43:57 PM
 */
public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final CounterService counterService;

    public IncrementAndGetRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    /**
     * 接口泛型：处理IncrementAndGetRequest类型的请求
     *
     * @param rpcCtx  the rpc context
     * @param request the request
     */
    @Override
    public void handleRequest(final RpcContext rpcCtx, final IncrementAndGetRequest request) {
        // Closure，当后续步骤执行完成后，通过RpcContext响应客户端
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                // 调用CounterClosure抽象类的getValueResponse方法，获取下面incrementAndGet执行的时候塞入的response
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.counterService.incrementAndGet(request.getDelta(), closure);
    }

    //返回关注的请求的类型
    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
