package com.qingbo.dailyutils.distributionlock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * @Auther: gaoqingbo
 * @Date: 2019/4/20 19:04
 * @Description:
 */
public class LockWatcher implements Watcher {
    private CountDownLatch countDownLatch ;
    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType()== Event.EventType.NodeDeleted){
            countDownLatch.countDown();
        }
    }

    public LockWatcher(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }
}
