package com.zkQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.DistributedDelayQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName zkDelayQueueTest
 * @Description zk 延时队列,延时指定时间后消费数据
 * @Author 贺楚翔
 * @Date 2020-06-01 15:04
 * @Version 1.0
 **/
public class zkDelayQueueTest {
    private static final String PATH = "/example/queue";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        DistributedDelayQueue<String> queue = null;

        client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(3000,3));
        client.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("监听状态："+event.getType().name());
            }
        });
        client.start();
        QueueConsumer<String> consumer = createConsumer();
        QueueBuilder<String> builder = QueueBuilder.builder(client,consumer,createSerializer(),PATH);
        queue = builder.buildDelayQueue();
        queue.start();
        for (int i = 0; i < 10; i++) {
            queue.put("test-"+i,System.currentTimeMillis()+1000);
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

        System.out.println(simpleDateFormat.format(new Date().getTime())+":already put all items");
        Thread.sleep(2000);

        CloseableUtils.closeQuietly(client);
        CloseableUtils.closeQuietly(queue);
    }

    private static QueueSerializer<String> createSerializer() {
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

    private static QueueConsumer<String> createConsumer() {
        return new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) throws Exception {
                System.out.println("消费一条数据："+ message);
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("状态："+ newState.name());
            }
        };
    }
}
