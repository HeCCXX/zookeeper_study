package com.leaderlatch;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName LeaderLatchTest
 * @Description curator  leader 选举方法，lead latch
 * 首先我们创建了10个LeaderLatch，启动后它们中的一个会被选举为leader。
 * 因为选举会花费一些时间，start后并不能马上就得到leader。
 * 通过hasLeadership查看自己是否是leader， 如果是的话返回true。
 * 可以通过.getLeader().getId()可以得到当前的leader的ID。
 * 只能通过close释放当前的领导权。
 * await是一个阻塞方法， 尝试获取leader地位，但是未必能上位。
 * @Author 贺楚翔
 * @Date 2020-06-19 13:28
 * @Version 1.0
 **/
public class LeaderLatchTest {
    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/example/leader";

    public static void main(String[] args) throws Exception {
        List<CuratorFramework> clients = Lists.newArrayList();
        List<LeaderLatch> examples = Lists.newArrayList();
        final TestingServer server = new TestingServer();
        try {

            for (int i = 0; i < CLIENT_QTY; i++) {
                final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
                clients.add(client);
                final LeaderLatch leaderLatch = new LeaderLatch(client, PATH, "Client #" + i);
                examples.add(leaderLatch);
                client.start();
                leaderLatch.start();
            }
            Thread.sleep(20000);
            LeaderLatch leaderLatch = null;
            for (int i = 0; i < CLIENT_QTY; i++) {
                final LeaderLatch example = examples.get(i);
                //检查是否为leader
                if (example.hasLeadership()){
                    leaderLatch = example;
                }
            }
            System.out.println("Current leader is :" + leaderLatch.getId());
            System.out.println("release the leader : " + leaderLatch.getId());

            leaderLatch.close();
            examples.get(0).await(2, TimeUnit.SECONDS);
            System.out.println("Client #0 maybe is elected as the leader or not although it want to be");
            System.out.println("the new leader is "+ examples.get(0).getLeader().getId());

            System.out.println("press enter to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("shutting down...");
            for (LeaderLatch example : examples) {
                //Already closed or has not been started 由于上面关闭了一个leader，所以该处会报异常
                CloseableUtils.closeQuietly(example);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            CloseableUtils.closeQuietly(server);
        }

    }
}
