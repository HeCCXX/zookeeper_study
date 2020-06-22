package com.lock.readwritelock;

import com.lock.reentrant.FakeLimitResource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName ExampleClientReadWriteLocks
 * @Description 分布式读写锁
 * @Author 贺楚翔
 * @Date 2020-06-22 15:46
 * @Version 1.0
 **/
public class ExampleClientReadWriteLocks {
    private final InterProcessReadWriteLock lock;
    private final InterProcessMutex readLock;
    private final InterProcessMutex writeLock;
    private final FakeLimitResource resource;
    private final String clientName;

    public ExampleClientReadWriteLocks(CuratorFramework client, String lockpath, FakeLimitResource resource,String clientName) {
        this.clientName = clientName;
        this.lock = new InterProcessReadWriteLock(client,lockpath);
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.resource = resource;
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!writeLock.acquire(time,unit)){
            throw new IllegalStateException(clientName + " could not acquire the writeLock");
        }
        System.out.println(clientName + " has the writeLock");
        if (!readLock.acquire(time,unit)){
            throw new IllegalStateException(clientName + " could not acquire the readLock");
        }
        System.out.println(clientName + " has the readLock");
        try {
            resource.use();
        }finally {
            System.out.println(clientName + "releasing the lock");
            readLock.release();
            writeLock.release();
        }
    }
}
