package com.frank.redis;

import com.frank.redis.listener.RTopicListener;
import lombok.extern.slf4j.Slf4j;
import org.redisson.Redisson;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

/**
 * This is test demo
 */
@Slf4j
public class redissionDemo {

    public static void main(String[] args) {
        Config config = new Config();
        config.useSingleServer().setAddress("//YOUR REDIS HOST:PORT");
        RedissonClient redisson = Redisson.create(config);

        LocalCachedMapOptions options = LocalCachedMapOptions.defaults()
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.NONE)
                .cacheSize(1000)
                .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.NONE)
                .syncStrategy(LocalCachedMapOptions.SyncStrategy.UPDATE)
                .timeToLive(1000, TimeUnit.SECONDS)
                .maxIdle(1000, TimeUnit.SECONDS);

        RLocalCachedMap<String,String> rLocalCachedMap = redisson.getLocalCachedMap("RTopic",options);

        RTopicListener rTopicListener = new RTopicListener((Redisson) redisson);

        log.info("Start!");

        int updateTime = 0;

         new Thread(){
             @Override
             public void run() {
                 super.run();
                 while(true)
                 {
//                     rLocalCachedMap.put("111",String.valueOf(updateTime)+"::::"+String.valueOf(System.currentTimeMillis()));
                     try {
                         Thread.sleep(100000);
                     } catch (InterruptedException e) {
                         e.printStackTrace();
                     }
                 }
             }
         }.start();


    }
    

}
