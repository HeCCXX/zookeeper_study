package com.distributionbarrier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ZKBarrierTest
 * @Description 栅栏Barrier,类似juc 的 CyclicBarrier，它会阻塞所有节点上的等待线程，直到某一条件达到，然后让所有节点恢复运行
 * 在本实例中，创建barrier来设置和移除栅栏，创建5个线程，在各个线程的barrier1中阻塞，最后移除barrier，运行所有线程运行
 * @Author 贺楚翔
 * @Date 2020-06-17 14:38
 * @Version 1.0
 **/
public class ZkBarrierTest {
    private static final int QTY = 5;
    private static final String PATH = "/example/barrier";
    public static void main(String[] args) {
        try(TestingServer server = new TestingServer()) {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),new ExponentialBackoffRetry(2000,3));
            client.start();
            final ExecutorService executorService = Executors.newFixedThreadPool(QTY);
            final DistributedBarrier barrier = new DistributedBarrier(client, PATH);
            //设置barrier
            barrier.setBarrier();
            for (int i = 0; i < QTY; i++) {
                final DistributedBarrier barrier1 = new DistributedBarrier(client, PATH);
                int index = i;
                executorService.submit(()->{
                    try {
                        Thread.sleep((long) (3*Math.random()));
                        System.out.println("client #"+index + "waits on barrier");
                        //线程到达等待
                        barrier1.waitOnBarrier();
                        System.out.println("client #"+index + "begin");
                        return null;
                    } catch (InterruptedException e) {

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });

            }
            Thread.sleep(10000);
            System.out.println("all Barrier instances should wait the condition");
            //移除barrier让所有节点通过，恢复运行
            barrier.removeBarrier();
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
