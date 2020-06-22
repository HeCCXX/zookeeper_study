package com.lock.readwritelock;


import com.lock.reentrant.ExampleClientThatLocks;
import com.lock.reentrant.FakeLimitResource;
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
 * @Description curator 实现分布式读写锁
 * 在这个例子中，首先请求一个写锁，然后降级为读锁，执行业务逻辑，然后释放读写锁
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
                    // 每个线程创建一个client
                    final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
                    try {
                        client.start();
                        final ExampleClientReadWriteLocks example = new ExampleClientReadWriteLocks(client, PATH, fakeLimitResource, "Client #" + index);
                        //每个线程重复50次获取资源，输出结果
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
