package com.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName ExampleClientThatLocks
 * @Description 负责请求锁，使用资源、释放锁一个完整的访问过程
 * @Author 贺楚翔
 * @Date 2020-06-22 14:26
 * @Version 1.0
 **/
public class ExampleClientThatLocks {
    private final InterProcessMutex lock;
    private final FakeLimitResource resource;
    private final String clientName;

    public ExampleClientThatLocks(CuratorFramework client,String lockpath,FakeLimitResource resource,String clientName) {
       this.resource = resource;
       this.clientName = clientName;
        lock = new InterProcessMutex(client, lockpath);
    }

    /**
    * 获取锁、访问资源、释放锁，获取锁利用acquire(long time, TimeUnit unit)设置延时获取，当等待时间结束
    * blocks until it's available or the given time expires 阻塞直到可用或者等待时间到达
    * @param time
    * @param unit
    * @return void
    * @exception
    **/
    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time,unit)){
            throw new IllegalStateException(clientName + " could not acquire this lock");
        }
        try {
            System.out.println(clientName + " has the lock");
            resource.use();
        } finally {
            System.out.println(clientName + " releasing the lock ");
            lock.release();
        }
    }
}
