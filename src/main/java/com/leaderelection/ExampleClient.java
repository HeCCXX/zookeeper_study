package com.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName ExampleClient
 * @Description exampleClient类，它继承LeaderSelectorListenerAdapter类，实现takeLeadership方法
 * 类似LeaderLatch，election也需要调用start方法，一旦启动，当实例取得领导权时你的listener
 * 的takeleadership方法被调用，而takeleadership方法只有领导权被释放时才返回。当不适用LeaderSelector
 * 实例时，应该调用它的close方法。
 * @Author 贺楚翔
 * @Date 2020-06-19 14:10
 * @Version 1.0
 **/
public class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {
    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public ExampleClient(String name, String path,CuratorFramework client) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start(){
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        final int waitSeconds = (int) (5 * Math.random() +1);
        System.out.println(name + "is now the leader.Waiting "+waitSeconds + "seconds...");
        System.out.println(name + "has been leader   " + leaderCount.getAndIncrement() +"time(s) before");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            System.err.println(name + "was interrupted");
            Thread.currentThread().interrupt();
        }finally {
            System.out.println(name + " relinquishing leadership.\n");
        }


    }
}
