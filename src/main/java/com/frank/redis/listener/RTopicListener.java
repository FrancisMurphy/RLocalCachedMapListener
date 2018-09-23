package com.frank.redis.listener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.RedissonTopic;
import org.redisson.api.RTopic;
import org.redisson.cache.*;
import org.redisson.client.codec.Codec;

import java.io.IOException;

/**
 * This is a very simple example about how register a custom listener by RTopic(Distributed topic.
 * Messages are delivered to all message listeners across Redis cluster) to monitor every RLocalCachedMap instance.
 * This listener can get the KEY and VALUE this in target map directly;
 * FYI @see https://github.com/redisson/redisson/wiki/7.-distributed-collections
 */
@Slf4j
public class RTopicListener {

    private RTopic<Object> invalidationTopic;

    private Redisson redisson;

    private int syncListenerId;

    private Codec codec;

    public RTopicListener(Redisson redissonClient) {

        this.redisson = redissonClient;
        this.codec = redisson.getCommandExecutor().getConnectionManager().getCodec();

        this.invalidationTopic = new RedissonTopic<Object>(LocalCachedMessageCodec.INSTANCE,
                redisson.getCommandExecutor(), "{RTopic}:topic");
        syncListenerId = invalidationTopic.addListener((channel, msg) -> {
            if (msg instanceof LocalCachedMapDisable) {
            }

            if (msg instanceof LocalCachedMapEnable) {
            }

            if (msg instanceof LocalCachedMapClear) {
            }

            if (msg instanceof LocalCachedMapInvalidate) {
            }

            if (msg instanceof LocalCachedMapUpdate) {
                LocalCachedMapUpdate updateMsg = (LocalCachedMapUpdate) msg;

                for (LocalCachedMapUpdate.Entry entry : updateMsg.getEntries()) {
                    ByteBuf keyBuf = Unpooled.wrappedBuffer(entry.getKey());
                    ByteBuf valueBuf = Unpooled.wrappedBuffer(entry.getValue());
                    try {
                        Object key = codec.getMapKeyDecoder().decode(keyBuf, null);
                        Object value = codec.getMapValueDecoder().decode(valueBuf, null);
                        log.info("####################UPDATE##########################");
                        log.info("Receive RTopic:{} of update message:[KEY:{}] [VALUE:{}]",channel,key.toString(),value.toString());
                    } catch (IOException e) {
                        log.error("Can't decode map entry", e);
                    } finally {
                        keyBuf.release();
                        valueBuf.release();
                    }
                }

            }
        });

    }
}
