package com.qingbo.dailyutils.distributionlock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @Auther: gaoqingbo
 * @Date: 2019/4/20 17:03
 * @Description: 建立客户端连接
 */
public class ZookeeperClient {
    private static final String ZK_Addr="120.26.232.253:2181";



    private static int sessionTimeOut=5000;

    /**
     * 获取连接
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static ZooKeeper getInstance() throws IOException, InterruptedException {

      final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(ZK_Addr, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        return zooKeeper;
    }
    public static int getSessionTimeOut() {
        return sessionTimeOut;
    }

}
