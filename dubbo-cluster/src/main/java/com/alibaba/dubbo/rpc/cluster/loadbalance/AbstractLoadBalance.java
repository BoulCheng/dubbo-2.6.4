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
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.cluster.LoadBalance;

import java.util.List;

/**
 * 对方法调用的负载均衡
 * AbstractLoadBalance
 *
 */
public abstract class AbstractLoadBalance implements LoadBalance {

    /**
     * calculateWarmupWeight 是重新计算权重值的方法，计算公式为： 服务运行时长/(预热时长/设置的权重值)，等价于 (服务运行时长/预热时长)*设置的权重值
     * 重新计算后的权重值为 1 ~ 设置的权重值，运行时间越长，计算出的权重值越接近设置的权重值
     * 预热时长和设置的权重值不变，服务运行时间越长，计算出的值越接近 weight，但不会等于 weight。在返回计算后的权重结果中，对小于1和大于设置的权重值进行了处理，当重新计算后的权重小于1时返回1；处于1和设置的权重值之间时，直接返回计算后的结果；当权重大于设置的权重值时（因为条件限制，不会出现该类情况），返回设置的权重值
     * @param uptime
     * @param warmup
     * @param weight
     * @return
     */
    static int calculateWarmupWeight(int uptime, int warmup, int weight) {
        int ww = (int) ((float) uptime / ((float) warmup / (float) weight));
        return ww < 1 ? 1 : (ww > weight ? weight : ww);
    }

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        if (invokers == null || invokers.isEmpty())
            return null;
        if (invokers.size() == 1)
            return invokers.get(0);
        return doSelect(invokers, url, invocation);
    }

    protected abstract <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation);

    /**
     * getWeight 中获取当前权重值，通过 URL 获取当前 Invoker 设置的权重，如果当前服务提供者启动时间小于预热时间，则会重新计算权重值，对服务进行降权处理，保证服务能在启动初期不分发设置比例的全部流量，健康运行下去。
     * @param invoker
     * @param invocation
     * @return
     */
    protected int getWeight(Invoker<?> invoker, Invocation invocation) {
        // 获取当前Invoker配置的权重值
        int weight = invoker.getUrl().getMethodParameter(invocation.getMethodName(), Constants.WEIGHT_KEY, Constants.DEFAULT_WEIGHT);
        if (weight > 0) {
            // 服务启动时间
            long timestamp = invoker.getUrl().getParameter(Constants.REMOTE_TIMESTAMP_KEY, 0L);
            if (timestamp > 0L) {
                // 服务已运行时长
                int uptime = (int) (System.currentTimeMillis() - timestamp);
                // 服务预热时间，默认 DEFAULT_WARMUP = 10 * 60 * 1000 ，预热十分钟
                int warmup = invoker.getUrl().getParameter(Constants.WARMUP_KEY, Constants.DEFAULT_WARMUP);
                if (uptime > 0 && uptime < warmup) {
                    // 如果服务运行时长小于预热时长，则降权重新计算出预热时期的权重
                    weight = calculateWarmupWeight(uptime, warmup, weight);
                }
            }
        }
        return weight;
    }

}
