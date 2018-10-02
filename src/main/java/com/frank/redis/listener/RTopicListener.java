package com.frank.redis.listener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.RedissonTopic;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RTopic;
import org.redisson.cache.*;
import org.redisson.client.codec.Codec;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

            /*
             * The LocalCachedMapDisable{@link LocalCachedMapDisable} is used to receive disable notify when perform
             * transactional operations in other RLocalCachedMap{@link org.redisson.api.RLocalCachedMap};
             * The notify key is custom key(A custom expansion of hash bit operation){@link CacheKey}, which can
             * be get by a custom expansion of hash bit operation, so we need maintain this unidirection link to
             * mapping the notify key to real key what can be recognize.
             * FYI: The CacheKey can get from RMap{@link org.redisson.RedissonLocalCachedMap#get(Object)};
             */
            if (msg instanceof LocalCachedMapDisable) {
                LocalCachedMapDisable m = (LocalCachedMapDisable) msg;
                Set<CacheKey> keysToDisable = new HashSet<CacheKey>();
                for (byte[] keyHash : m.getKeyHashes()) {
                    CacheKey key = new CacheKey(keyHash);
                    keysToDisable.add(key);
                }
                log.info("####################Transcation notify##########################");
                log.info("Receive RTopic:{} of update message:[KEY HASH:{}]",channel,keysToDisable.toString());
                //disableKeys(requestId, keysToDisable, m.getTimeout());
            }

            if (msg instanceof LocalCachedMapEnable) {
            }

            if (msg instanceof LocalCachedMapClear) {
            }

            /**
             * It is worth noting that LocalCachedMapInvalidate only take effect on INVALIDATE{@link LocalCachedMapOptions.SyncStrategy.INVALIDATE},
             * and we only concern UPDATE{@link LocalCachedMapOptions.SyncStrategy.UPDATE}
             */
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
