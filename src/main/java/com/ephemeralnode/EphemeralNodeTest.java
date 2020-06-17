package com.ephemeralnode;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName EphemeralNodeTest
 * @Description 临时节点PersistentEphemeralNode,当session断开连接时，临时节点被删除
 * 高版本的curator包中该类为过时的
 * @Author 贺楚翔
 * @Date 2020-06-17 16:32
 * @Version 1.0
 **/
public class EphemeralNodeTest {
    private static final String PATH = "/example/node";
    private static final String PATH2 = "/example/ephemeralNode";
    public static void main(String[] args) throws Exception {
        final TestingServer server = new TestingServer();
        CuratorFramework client = null;
        PersistentEphemeralNode node  = null;
        client = CuratorFrameworkFactory.newClient(server.getConnectString(),new ExponentialBackoffRetry(2000,3));
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("client state : " + newState.name());
            }
        });
        client.start();

        node = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL,PATH2,"TEST".getBytes());
        node.start();
        node.waitForInitialCreate(3, TimeUnit.SECONDS);
        final String actualPath = node.getActualPath();
        System.out.println("node :" + actualPath + "   value : "+new String(client.getData().forPath(actualPath)));

         client.create().forPath(PATH, "persistent node ".getBytes());
        System.out.println("node :"+ PATH + "   value : " + new String(client.getData().forPath(PATH)));
        KillSession.kill(client.getZookeeperClient().getZooKeeper(),server.getConnectString());
        System.out.println("node : "+actualPath+"do not exist   " + (client.checkExists().forPath(PATH2)==null));
        System.out.println("node : "+PATH+"  value : "+new String(client.getData().forPath(PATH)));

        CloseableUtils.closeQuietly(server);
        CloseableUtils.closeQuietly(node);
        CloseableUtils.closeQuietly(client);
    }
}
