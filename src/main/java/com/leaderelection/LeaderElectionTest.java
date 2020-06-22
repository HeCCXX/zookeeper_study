package com.leaderelection;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * @ClassName LeaderElectionTest
 * @Description curator 提供的另外一种选举方式，Leader Election
 * 可以在takeleadership进行任务的分配等等，不需要返回，如果想要的此实例一直是leader的话可以
 * 加一个死循环，leaderSelector.autoRequeue()保证在此实例释放领导权之后还可能获得领导权。
 * 在本次实例中，我们使用AtomicInteger来记录获取领导权的次数，是公平策略。
 *
 * 与LeaderLatch不同的是，每个节点都可能变成leader，而Latch除非调用close，否则不会释放领导权。
 * @Author 贺楚翔
 * @Date 2020-06-19 14:08
 * @Version 1.0
 **/
public class LeaderElectionTest {
    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/example/leader";

    public static void main(String[] args) throws Exception {
        List<CuratorFramework> clients = Lists.newArrayList();
        List<ExampleClient> examples = Lists.newArrayList();
        final TestingServer server = new TestingServer();
        try {
            for (int i = 0; i < CLIENT_QTY; i++) {
                final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
                clients.add(client);
                final ExampleClient example = new ExampleClient("Client #" + i, PATH, client);
                examples.add(example);
                client.start();
                example.start();
            }

            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Shutting down...");
            for (ExampleClient example : examples) {
                CloseableUtils.closeQuietly(example);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            CloseableUtils.closeQuietly(server);
        }
    }
}
