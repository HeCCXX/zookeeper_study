package com.zkCount;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName ZKCountTest
 * @Description zookeeper 计数器
 * @Author 贺楚翔
 * @Date 2020-06-05 10:48
 * @Version 1.0
 **/
public class ZKCountTest implements SharedCountListener {
    private static final String PATH = "/example/counter";
    private static final int QTY = 5;
    @Override
    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
        System.out.println("Count value is Changed to "+newCount);
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        System.out.println("State Changed : " + newState.name());
    }

    public static void main(String[] args) throws Exception {
        final Random rand = new Random();
        ZKCountTest example = new ZKCountTest();
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(3000,3));
        client.start();
        SharedCount sharedCount = new SharedCount(client, PATH, 0);
        sharedCount.addListener(example);
        sharedCount.start();

        ArrayList<SharedCount> examples = Lists.newArrayList();
        ExecutorService executorService = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; i++) {
            SharedCount count = new SharedCount(client, PATH, 0);
            examples.add(count);
            executorService.submit(()->{
                try {
                    count.start();
                    Thread.sleep(rand.nextInt(10000));
                    System.out.println("Increment:"+count.trySetCount(count.getVersionedValue(),count.getCount()+rand.nextInt(20)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);

        for (int i = 0; i < QTY; i++) {
            examples.get(i).close();
        }
        sharedCount.close();
    }
}
