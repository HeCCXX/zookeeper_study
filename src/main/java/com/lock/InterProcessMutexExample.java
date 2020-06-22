package com.lock;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName InterProcessMutexExample
 * @Description curator 实现可重用分布式锁
 * 1、该代码实现生成5个client，每个client重复50次，请求锁、访问资源、释放锁的过程。每个client都在独立的线程中，结果可以看到
 * 锁被每个实例排他性的使用。
 * 2、该锁是可重用的，在一个线程中多次调用acquire，在线程拥有它是总是返回true。
 * 3、不应该在多个线程用同一个InterProcessMutex，可以在每个线程中都生成一个InterPorcessMutex实例，它们的path一样，这样它们
 * 可以拥有同一个锁。
 * @Author 贺楚翔
 * @Date 2020-06-22 14:33
 * @Version 1.0
 **/
public class InterProcessMutexExample {
    private static final int QTY = 5;
    private static final int REPETITIONS = QTY * 10;
    private static final String PATH = "/example/lock";

    public static void main(String[] args) throws Exception {
        final FakeLimitResource fakeLimitResource = new FakeLimitResource();
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(QTY, QTY, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        final TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < QTY; i++) {
                final int index = i;
                executor.submit(()->{
                    final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
                    try {
                        client.start();
                        final ExampleClientThatLocks example = new ExampleClientThatLocks(client, PATH, fakeLimitResource, "Client #" + index);
                        for (int j = 0; j < REPETITIONS; j++) {
                            example.doWork(10,TimeUnit.SECONDS);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }finally {
                        CloseableUtils.closeQuietly(client);
                    }
                    return null;
                });
            }
            executor.shutdown();
            executor.awaitTermination(10,TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }
}
