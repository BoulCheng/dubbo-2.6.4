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
package com.alibaba.dubbo.remoting.transport.netty;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractChannel;

import org.jboss.netty.channel.ChannelFuture;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * NettyChannel.
 */
final class NettyChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class);

    private static final ConcurrentMap<org.jboss.netty.channel.Channel, NettyChannel> channelMap = new ConcurrentHashMap<org.jboss.netty.channel.Channel, NettyChannel>();

    private final org.jboss.netty.channel.Channel channel;

    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    private NettyChannel(org.jboss.netty.channel.Channel channel, URL url, ChannelHandler handler) {
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
    }

    static NettyChannel getOrAddChannel(org.jboss.netty.channel.Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }
        NettyChannel ret = channelMap.get(ch);
        if (ret == null) {
            NettyChannel nc = new NettyChannel(ch, url, handler);
            if (ch.isConnected()) {
                ret = channelMap.putIfAbsent(ch, nc);
            }
            if (ret == null) {
                ret = nc;
            }
        }
        return ret;
    }

    static void removeChannelIfDisconnected(org.jboss.netty.channel.Channel ch) {
        if (ch != null && !ch.isConnected()) {
            channelMap.remove(ch);
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.getLocalAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    /**
     * 调用接口最终都会通过netty发送 封装好的调用信息 (包括调用的接口的全限定名、调用的方法名、方法参数等)
     *
     * send:95, NettyChannel (com.alibaba.dubbo.remoting.transport.netty)
     * send:265, AbstractClient (com.alibaba.dubbo.remoting.transport)
     * send:53, AbstractPeer (com.alibaba.dubbo.remoting.transport)
     * request:116, HeaderExchangeChannel (com.alibaba.dubbo.remoting.exchange.support.header)
     * request:90, HeaderExchangeClient (com.alibaba.dubbo.remoting.exchange.support.header)
     * request:83, ReferenceCountExchangeClient (com.alibaba.dubbo.rpc.protocol.dubbo)
     * doInvoke:95, DubboInvoker (com.alibaba.dubbo.rpc.protocol.dubbo)
     * invoke:154, AbstractInvoker (com.alibaba.dubbo.rpc.protocol)
     * invoke:75, MonitorFilter (com.alibaba.dubbo.monitor.support)
     * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
     * invoke:54, FutureFilter (com.alibaba.dubbo.rpc.protocol.dubbo.filter)
     * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
     * invoke:49, ConsumerContextFilter (com.alibaba.dubbo.rpc.filter)
     * invoke:72, ProtocolFilterWrapper$1 (com.alibaba.dubbo.rpc.protocol)
     * invoke:77, ListenerInvokerWrapper (com.alibaba.dubbo.rpc.listener)
     * invoke:56, InvokerWrapper (com.alibaba.dubbo.rpc.protocol)
     * doInvoke:78, FailoverClusterInvoker (com.alibaba.dubbo.rpc.cluster.support)
     * invoke:244, AbstractClusterInvoker (com.alibaba.dubbo.rpc.cluster.support)
     * invoke:75, MockClusterInvoker (com.alibaba.dubbo.rpc.cluster.support.wrapper)
     * invoke:52, InvokerInvocationHandler (com.alibaba.dubbo.rpc.proxy)
     * {MethodName}:-1, proxy1 (com.alibaba.dubbo.common.bytecode)
     *
     * @param message
     * @param sent
     * @throws RemotingException
     */
    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        super.send(message, sent);

        boolean success = true;
        int timeout = 0;
        try {
            ChannelFuture future = channel.write(message);
            //一些特殊场景下，为了尽快调用返回，可以设置是否等待消息发出：
            //sent="true" 等待消息发出，消息发送失败将抛出异常；
            //sent="false" 不等待消息发出，将消息放入 IO 队列，即刻返回。
            if (sent) {
                timeout = getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
                success = future.await(timeout);
            }
            Throwable cause = future.getCause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + ", cause: " + e.getMessage(), e);
        }

        if (!success) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress()
                    + "in timeout(" + timeout + "ms) limit");
        }
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            channel.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        if (value == null) { // The null value unallowed in the ConcurrentHashMap.
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NettyChannel other = (NettyChannel) obj;
        if (channel == null) {
            if (other.channel != null) return false;
        } else if (!channel.equals(other.channel)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

}
