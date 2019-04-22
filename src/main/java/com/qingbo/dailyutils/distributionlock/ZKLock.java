package com.qingbo.dailyutils.distributionlock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: gaoqingbo
 * @Date: 2019/4/20 17:06
 * @Description:
 */
public class ZKLock {
    //根节点
    private static final String Root_LOCK="/LOCKS";
    private ZooKeeper zooKeeper;
    private int sessionTimeout;
    private String lockId;//记录当前锁节点ID
    private CountDownLatch countDownLatch = new CountDownLatch(1);//used by lockWatcher

    private  final static byte[] data = {1,2};// data on node
    public ZKLock() throws IOException, InterruptedException {
        this.zooKeeper = ZookeeperClient.getInstance();
        this.sessionTimeout = ZookeeperClient.getSessionTimeOut();
    }


    //获取锁
    public boolean lock(){

        try {
            //1 create root lock and write node data
            lockId = zooKeeper.create(Root_LOCK+"/",data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(Thread.currentThread().getName()+"create node success!node id:["+lockId+"], start to compete lock...");
            //[smallest note can acquire the lock ]
            //2 check current node is the smallest node
            List<String> children = zooKeeper.getChildren(Root_LOCK, true);//first, get all chile node
            SortedSet<String> sortedSet = new TreeSet<>(); //then do sort with asc
            for (String child: children) {
                sortedSet.add(Root_LOCK+"/"+child);
            }
            String first = sortedSet.first();//get first smallest node
            if (lockId.equals(first)){
                //current node is the smallest
                System.out.println(Thread.currentThread().getName()+"get lock success, node id :["+lockId+"]");
                return true;
            }
            // if current node is not smallest,get one ahead of current
            SortedSet<String> headSet = ((TreeSet<String>) sortedSet).headSet(lockId);
            if (!headSet.isEmpty()){
                String headLastOne = headSet.last();
                // monitor the headLast node ,if session time out or node deleted,means current node is the smallest,it held the lock
                zooKeeper.exists(headLastOne,new LockWatcher(countDownLatch));
                countDownLatch.await(sessionTimeout, TimeUnit.MILLISECONDS);
                System.out.println(Thread.currentThread().getName()+"get lock success, node id :["+lockId+"]");
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return  false;
    }

    //释放锁
    public boolean unlock() throws KeeperException, InterruptedException {
        System.out.println(Thread.currentThread().getName()+"unlock start...");
        zooKeeper.delete(lockId,-1);
        System.out.println(Thread.currentThread().getName()+"delete node finished,lockId = ["+lockId+"]");
        return false;
    }


    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(()->{

            });

        }
    }
}
