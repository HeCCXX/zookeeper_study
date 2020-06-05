package com.zkCache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

/**
 * @ClassName zkTreeCacheTest
 * @Description treeCache 可以监控该节点和它的子节点，类似PathChildrenCache和NodeCache的结合
 * @Author 贺楚翔
 * @Date 2020-06-05 9:30
 * @Version 1.0
 **/
public class zkTreeCacheTest {
    private static final String PATH = "/example/treeCache";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        TreeCache cache = null;

        client = CuratorFrameworkFactory.newClient("192.168.145.110:2181",new ExponentialBackoffRetry(3000,3));
        client.start();
        cache = new TreeCache(client,PATH);
        cache.start();
        processCommands(client,cache);
    }

    private static void processCommands(CuratorFramework client, TreeCache cache) throws Exception {
        printHelper();
        addListener(cache);
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done){
            System.out.println(">");
            String line = bf.readLine();
            if (line == null){
                break;
            }
            String command = line.trim();
            String[] split = command.split("\\s");
            if (split.length == 0){
                continue;
            }
            String operation = split[0];
            String[] args = Arrays.copyOfRange(split, 1, split.length);
            if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?")) {
                printHelper();
            } else if (operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit")) {
                done = true;
            } else if (operation.equals("set")) {
                setValue(client, command, args);
            } else if (operation.equals("remove")) {
                remove(client, command, args);
            } else if (operation.equals("list")) {
                list(cache);
            }
            Thread.sleep(1000);
        }
    }

    private static void list(TreeCache cache) {
        if (cache.getCurrentChildren(PATH).size() == 0){
            System.out.println("cache is empty!");
        }else {
            for (Map.Entry<String, ChildData> stringChildDataEntry : cache.getCurrentChildren(PATH).entrySet()) {
                System.out.println(stringChildDataEntry.getKey()+"===="+new String(stringChildDataEntry.getValue().getData()));
            }
        }
    }

    private static void remove(CuratorFramework client, String command, String[] args) {
        if (args.length != 1){
            System.err.println("syntax error (excepted remove <path>)"+command);
        }
        String node = args[0];
        if (node.contains("/")){
            System.err.println("Invalid node path");
            return;
        }
        String path = ZKPaths.makePath(PATH, node);
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            
        }
    }

    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 2){
            System.err.println("syntax error(excepted set <path> value)" + command);
        }
        String name = args[0];
        byte[] value = args[1].getBytes();
        if (name.contains("/")){
            System.err.println("Invalid node name "+name);
            return;
        }
        String s = ZKPaths.makePath(PATH, name);
        try {
            client.setData().forPath(s,value);
        } catch (Exception e) {
            client.create().creatingParentsIfNeeded().forPath(s,value);
        }
    }

    private static void addListener(TreeCache cache) {
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                switch (event.getType()){
                    case NODE_ADDED:
                        System.out.println("TreeNode added:"+ ZKPaths.getNodeFromPath(event.getData().getPath())+"value:"+new String(event.getData().getData()));
                        break;
                    case NODE_UPDATED:
                        System.out.println("TreeNode updated:"+ZKPaths.getNodeFromPath(event.getData().getPath())+"value:"+new String(event.getData().getData()));
                        break;
                    case NODE_REMOVED:
                        System.out.println("TreeNode delete:"+ ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    default:
                        System.out.println("other action:" + event.getType().name());
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
