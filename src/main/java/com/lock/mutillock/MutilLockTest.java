package com.lock.mutillock;

import com.lock.reentrant.FakeLimitResource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName MutilLockTest
 * @Description 多锁对象，是一个锁的容器
 * 当调用acquire方法，容器中的所有锁都会调用acquire方法，如果请求失败，所有锁都会调用release方法。
 * @Author 贺楚翔
 * @Date 2020-06-22 16:54
 * @Version 1.0
 **/
public class MutilLockTest {
    private static final String PATH1 = "/example/lock1";
    private static final String PATH2 = "/example/lock2";

    public static void main(String[] args) throws Exception {
        final FakeLimitResource fakeLimitResource = new FakeLimitResource();
        final TestingServer server = new TestingServer();
        try {
            final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();
            final InterProcessMutex lock1 = new InterProcessMutex(client, PATH1);
            final InterProcessSemaphoreMutex lock2 = new InterProcessSemaphoreMutex(client, PATH2);
            final InterProcessMultiLock multiLock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));
            if (!multiLock.acquire(10, TimeUnit.SECONDS)){
                throw new IllegalStateException("could not acquire the lock");
            }
            System.out.println("has the lock");
            System.out.println("has the lock1 " + lock1.isAcquiredInThisProcess());
            System.out.println("has the lock2 " + lock2.isAcquiredInThisProcess());

            try {
                fakeLimitResource.use();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("releasing the lock");
                multiLock.release();
            }
            System.out.println("has the lock1 " + lock1.isAcquiredInThisProcess());
            System.out.println("has the lock2 " + lock2.isAcquiredInThisProcess());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(server);
        }
    }
}
