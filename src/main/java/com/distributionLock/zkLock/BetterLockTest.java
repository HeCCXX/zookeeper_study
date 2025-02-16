package com.distributionLock.zkLock;

import com.google.common.base.Strings;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName BetterLockTest
 * @Description 解决羊群效应，利用getChildren
 * @Author 贺楚翔
 * @Date 2020-05-09 15:13
 * @Version 1.0
 **/
public class BetterLockTest {

    private String zkQurom = "192.168.145.110:2181";

    private String lockName = "/mylock";

    private String lockZnode = null;

    private ZooKeeper zk;

    public BetterLockTest(){
        try {
            zk = new ZooKeeper(zkQurom, 6000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("Receive event "+watchedEvent);
                    if(Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                        System.out.println("connection is established...");
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void ensureRootPath(){
        try {
            if (zk.exists(lockName,true)==null){
                zk.create(lockName,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取锁
     * @return
     * @throws InterruptedException
     */
    public void lock(){
        String path = null;
        ensureRootPath();
        try {
            path = zk.create(lockName+"/mylock_", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            lockZnode = path;
            List<String> minPath = zk.getChildren(lockName,false);
            System.out.println(minPath);
            Collections.sort(minPath);
            System.out.println(minPath.get(0)+" and path "+path);
            if (!Strings.nullToEmpty(path).trim().isEmpty()&&!Strings.nullToEmpty(minPath.get(0)).trim().isEmpty()&&path.equals(lockName+"/"+minPath.get(0))) {
                System.out.println(Thread.currentThread().getName() + "  get Lock...");
                return;
            }
            String watchNode = null;
            for (int i=minPath.size()-1;i>=0;i--){
                if(minPath.get(i).compareTo(path.substring(path.lastIndexOf("/") + 1))<0){
                    watchNode = minPath.get(i);
                    break;
                }
            }

            if (watchNode!=null){
                final String watchNodeTmp = watchNode;
                final Thread thread = Thread.currentThread();
                Stat stat = zk.exists(lockName + "/" + watchNodeTmp,new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                            System.out.println(thread.getName()+"interrupt...."+watchNodeTmp);
                            thread.interrupt();
                        }
                        try {
                            zk.exists(lockName + "/" + watchNodeTmp,true);
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                });
                if(stat != null){
                    System.out.println("Thread " + Thread.currentThread().getName() + " waiting for " + lockName + "/" + watchNode);
                }
            }
            try {
                Thread.sleep(1000000000);
            }catch (InterruptedException ex){
                System.out.println(Thread.currentThread().getName() + " notify");
                System.out.println(Thread.currentThread().getName() + "  get Lock...");
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放锁
     */
    public void unlock(){
        try {
            System.out.println(Thread.currentThread().getName() +  "release Lock..."+lockZnode);
            zk.delete(lockZnode,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }



    public static void main(String args[]) throws InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(10);
        for (int i = 0;i<4;i++){
            service.execute(()-> {
                BetterLockTest test = new BetterLockTest();
                try {
                    test.lock();
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                test.unlock();
            });
        }
        service.shutdown();
    }

}
