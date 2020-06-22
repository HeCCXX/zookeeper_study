package com.lock;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ClassName FakeLimitResource
 * @Description 分布式锁共享资源
 * @Author 贺楚翔
 * @Date 2020-06-22 14:19
 * @Version 1.0
 **/
public class FakeLimitResource {
    private final AtomicBoolean isUse = new AtomicBoolean(false);

    public void use(){
        //真实情况下我们会在这里访问、维护一个共享资源
        //这个例子在使用锁的情况下，不会非法并发异常IllegalStateException
        //但是在无锁的情况下由于sleep一段时间，很容易抛出异常
        if (isUse.compareAndSet(false,true)){
            throw new IllegalStateException("needs to be used by one client at a time");
        }
        try {
            Thread.sleep((long) (3 * Math.random()));
        } catch (InterruptedException e) {
            isUse.set(false);
        }
    }
}
