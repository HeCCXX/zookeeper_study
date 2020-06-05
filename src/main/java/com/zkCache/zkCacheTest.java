package com.zkCache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * @ClassName zkCacheTest
 * @Description zookeeper缓存实例，分为pathCache、nodeCache、TreeCache
 * @Author 贺楚翔
 * @Date 2020-06-01 16:07
 * @Version 1.0
 **/
public class zkCacheTest {
    private static final String PATH = "/example/cache";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        NodeCache cache = null;

        client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(3000,3));
        client.start();
        cache = new NodeCache(client,PATH);
        cache.start();
        processCommands(client,cache);

    }

    private static void processCommands(CuratorFramework client, NodeCache cache) throws IOException, InterruptedException {
        printHelper();
        addListener(cache);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done){
            System.out.println("> ");
            String line = in.readLine();
            if (line == null){
                break;
            }
            String command = line.trim();
            String[] split = command.split("\\s+");
            if (split.length == 0){
                continue;
            }
            String operation = split[0];
            String[] args = Arrays.copyOfRange(split, 1, split.length);
            if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?")){
                printHelper();
            }else if (operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit")){
                done = true;
            }else if (operation.equalsIgnoreCase("set")){
                setValue(client,command,args);
            }else if (operation.equalsIgnoreCase("remove")){
                remove(client);
            }else if (operation.equalsIgnoreCase("show")){
                show(cache);
            }
            Thread.sleep(2000);
        }
    }

    private static void show(NodeCache cache) {
        if (cache.getCurrentData() != null){
            System.out.println(cache.getCurrentData().getPath()+"---------"+new String(cache.getCurrentData().getData()));
        }else {
            System.out.println("cache do not have data");
        }
    }

    private static void remove(CuratorFramework client) {
        try {
            client.delete().forPath(PATH);
        } catch (Exception e) {

        }
    }

    private static void setValue(CuratorFramework client, String command, String[] args) {
        if (args.length != 1){
            System.err.println("syntax error (excepted set <value>)"+command);
            return;
        }
        byte[] bytes = args[0].getBytes();
        try {
            client.setData().forPath(PATH,bytes);
        } catch (Exception e) {
            try {
                client.create().creatingParentsIfNeeded().forPath(PATH,bytes);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static void addListener(NodeCache cache) {
        NodeCacheListener listener = new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                if (cache.getCurrentData() != null) {
                    System.out.println("Node 改变：" + cache.getCurrentData().getPath() + ",value:" + new String(cache.getCurrentData().getData()));
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private static void printHelper() {
            System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
            System.out.println("set <value>: Adds or updates a node with the given name");
            System.out.println("remove: Deletes the node with the given name");
            System.out.println("show: Display the node's value in the cache");
            System.out.println("quit: Quit the example");
            System.out.println();
    }
}
