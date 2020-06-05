package com.zkQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.queue.DistributedPriorityQueue;
import org.apache.curator.framework.recipes.queue.QueueBuilder;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.recipes.queue.QueueSerializer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

/**
 * @ClassName zk
 * @Description zk的优先级队列，put（T item, int priority），优先输入优先级较高的元素
 * @Author 贺楚翔
 * @Date 2020-06-01 14:44
 * @Version 1.0
 **/
public class zkPriorityQueueTest {
    private static final String Path = "/example/queue";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        DistributedPriorityQueue<String> queue = null;

        client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(3000,3));
        client.getCuratorListenable().addListener(new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("CuratorEvent:"+event.getType().name());
            }
        });

        client.start();
        QueueConsumer<String> consumer = createConsumer();
        QueueBuilder<String> builder = QueueBuilder.builder(client, consumer, createSerializer(), Path);
        queue = builder.buildPriorityQueue(0);
        queue.start();

        for (int i = 0; i < 10; i++) {
            //随机给优先级，当优先级越靠前，越先被消费
            int priority = (int) (Math.random() * 100);
            System.out.println("test-"+i+"priority:"+priority);
            queue.put("test-"+i,priority);
            Thread.sleep((long) (Math.random()*50));
        }
        Thread.sleep(2000);
        CloseableUtils.closeQuietly(client);
        CloseableUtils.closeQuietly(queue);
    }

    /**
    * 序列化和反序列化
    * @param
    * @return org.apache.curator.framework.recipes.queue.QueueSerializer<java.lang.String>
    * @exception       
    **/
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

    /**
    * 消费者
    * @param
    * @return org.apache.curator.framework.recipes.queue.QueueConsumer<java.lang.String>
    * @exception
    **/
    private static QueueConsumer<String> createConsumer() {
        return  new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String message) throws Exception {
                System.out.println("consumer a message:"+message);
            }

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("connection new State:"+newState.name());
            }
        };
    }
}
