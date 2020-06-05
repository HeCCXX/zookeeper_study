package com.distributionLock.zkLock;

import com.google.common.base.Strings;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @ClassName LockTest
 * @Description zookeeper分布式锁,利用watch会出现羊群效应
 * 需要导入zk依赖和guava依赖
 *          <dependency>
 *             <groupId>org.apache.zookeeper</groupId>
 *             <artifactId>zookeeper</artifactId>
 *             <version>3.4.8</version>
 *         </dependency>
 *
 *         <!-- https://mvnrepository.com/artifact/com.google.guava/guava -->
 *         <dependency>
 *             <groupId>com.google.guava</groupId>
 *             <artifactId>guava</artifactId>
 *             <version>28.1-jre</version>
 *         </dependency>
 * @Author 贺楚翔
 * @Date 2020-05-09 15:11
 * @Version 1.0
 **/
public class LockTest {
    private String url = "192.168.145.110:2181";

    private String lockFolder = "/mylock";

    private String nodeString = "/mylock/lock";

    private ZooKeeper zk;

    public LockTest() throws IOException {

        zk = new ZooKeeper(url, 60, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("receive:"+event);
                if (event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("connection is established");
                }
            }
        });

    }

    public void release(){
        try {
            zk.delete(nodeString,-1);
            System.out.println(Thread.currentThread().getName()+"release.......");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void lock(){
        ensureRootPath();
        watchNode(nodeString,Thread.currentThread());
        String path =null;
        while (true){
            try {
                path = zk.create(nodeString, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException e) {
                System.out.println(Thread.currentThread().getName()+"get lock fail....");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    System.out.println("action");
                    ex.printStackTrace();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!Strings.nullToEmpty(path).trim().isEmpty()){
                System.out.println(Thread.currentThread().getName()+"get lock.....");
                return;
            }
        }
    }

    private void watchNode(String nodeString, Thread currentThread)  {
        try {
            zk.exists(nodeString, event -> {
                System.out.println("=="+event.toString());
                if (event.getType() == Watcher.Event.EventType.NodeDeleted){
                    System.out.println(Thread.currentThread().getName()+"release lock!!!!");
                    currentThread.interrupt();
                }
            });
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void ensureRootPath() {
        try {
            if (zk.exists(lockFolder,true) == null){
                zk.create(lockFolder,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 4; i++) {
            executorService.execute(()->{
                LockTest test1 = null;
                try {
                    test1 = new LockTest();
                    test1.lock();
                    Thread.sleep(1000);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                test1.release();
            });
        }
        executorService.shutdown();
    }
}
