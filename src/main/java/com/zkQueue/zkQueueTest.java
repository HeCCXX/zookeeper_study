package com.zkQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

/**
 * @ClassName zkQueueTest
 * @Description zk  队列实例，包含普通DistributedQueue和 idDistributedQueue
 * idDistributedQueue可以根据id进行删除元素，而普通DistributedQueue没有删除
 * maven需要导入依赖包
 * <groupId>org.apache.curator</groupId>
 * <artifactId>curator-recipes</artifactId>
 *
 * <groupId>org.apache.curator</groupId>
 * <artifactId>curator-framework</artifactId>
 *
 * <groupId>org.apache.curator</groupId>
 * <artifactId>curator-client</artifactId>
 * @Author 贺楚翔
 * @Date 2020-05-27 10:04
 * @Version 1.0
 **/
public class zkQueueTest {
    private static final String Path = "/example/queue";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
//        DistributedQueue<String> queue = null;
        DistributedIdQueue<String> queue = null;

        client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(1000,3));
        client.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("CuratorEvent: "+ event.getType().name());
            }
        });
        client.start();
        QueueConsumer<String> consumer = createQueueConsumer();
        QueueBuilder<String> builder = QueueBuilder.builder(client, consumer, createQueueSerializer(), Path);
        queue = builder.buildIdQueue();
        queue.start();


        for (int i = 0; i < 10; i++) {
            queue.put("test---"+i,"Id"+i);
            Thread.sleep((long) (3 * Math.random()));
            queue.remove("Id"+i);
        }
        Thread.sleep(3000);

        CloseableUtils.closeQuietly(queue);
        CloseableUtils.closeQuietly(client);
    }

    private static QueueSerializer<String> createQueueSerializer() {
        return new QueueSerializer<String>() {
            @Override
            public byte[] serialize(String item) {
                return item.getBytes();
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }
        };
    }

    private static QueueConsumer<String> createQueueConsumer() {
        return new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) throws Exception {
                System.out.println("consumer one message:"+message);
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("connection new State:"+newState.name());
            }
        };
    }
}

