package com.lock.semaphore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName SemaphoreTet
 * @Description curator 实现semaphore
 * 下面例子中，首先获取5个租约，最后我们还给semaphore，接着请求一个租约，因为还有5个租约，所以请求可以满足，
 * 返回一个租约，还剩下4个租约，然后我们在请求5个租约，因为租约不够，阻塞到超时，还是不能满足，返回null。
 * @Author 贺楚翔
 * @Date 2020-06-22 16:36
 * @Version 1.0
 **/
public class SemaphoreTest {
    private static final int MAX_LEASE = 10;
    private static final String PATH = "/example/lock";

    public static void main(String[] args) throws Exception {
        final TestingServer server = new TestingServer();
        try {
            final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
            client.start();
            final InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, PATH, MAX_LEASE);
            final Collection<Lease> leases = semaphore.acquire(5);
            System.out.println("get " + leases.size() + " leases");
            final Lease lease = semaphore.acquire();
            System.out.println("get another lease");
            final Collection<Lease> leases1 = semaphore.acquire(5, 10, TimeUnit.SECONDS);
            System.out.println("Should timeout and acquire return " + leases1);
            System.out.println("return one lease");
            semaphore.returnLease(lease);
            System.out.println("return another 5 leases");
            semaphore.returnAll(leases);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(server);
        }


    }
}
