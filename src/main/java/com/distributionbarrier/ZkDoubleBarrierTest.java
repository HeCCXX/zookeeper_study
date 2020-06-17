package com.distributionbarrier;

import jdk.nashorn.internal.ir.CallNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ZkDoubleBarrierTest
 * @Description zookeeper double Barrier 双栅栏，类似田径赛跑，调用enter从第一个barrier触发，调用leave到达终点，
 * 当所有barrier完成，才结束。
 * @Author 贺楚翔
 * @Date 2020-06-17 15:16
 * @Version 1.0
 **/
public class ZkDoubleBarrierTest {
    private static final int QTY = 5;
    private static final String PATH = "/example/barrier";

    public static void main(String[] args) {
        try(final TestingServer server = new TestingServer()) {
            CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(),new ExponentialBackoffRetry(2000,3));
            client.start();
            final ExecutorService executorService = Executors.newFixedThreadPool(QTY);
            for (int i = 0; i < QTY; i++) {
                final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, PATH, QTY);
                int index = i;
                executorService.submit(()->{
                    try {
                        Thread.sleep((long) (3 * Math.random()));
                        System.out.println("client #" + index + "enter");
                        barrier.enter();
                        System.out.println("client #" + index + "begin");
                        Thread.sleep((long) (3000 * Math.random()));
                        barrier.leave();
                        System.out.println("client #" + index + "leave");
                        return null;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });
            }
            executorService.shutdown();
            executorService.awaitTermination(2, TimeUnit.MINUTES);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
