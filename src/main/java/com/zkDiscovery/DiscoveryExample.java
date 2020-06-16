package com.zkDiscovery;

import com.google.common.base.Predicate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.strategies.RandomStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * @ClassName DiscoveryExample
 * @Description 该类提供了增加、删除、修改、注册已有服务的服务
 * @Author 贺楚翔
 * @Date 2020-06-08 10:27
 * @Version 1.0
 **/
public class DiscoveryExample {
    private static final String PATH = "/example/discovery";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer();
        CuratorFramework client = null;
        ServiceDiscovery<InstanceDetails> serviceDiscovery = null;
        HashMap<String, ServiceProvider<InstanceDetails>> providers = Maps.newHashMap();
        try {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(),new ExponentialBackoffRetry(3000,3));
            client.start();
            JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<>(InstanceDetails.class);
            serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class).client(client).basePath(PATH).serializer(serializer).build();
            serviceDiscovery.start();
            processCommands(serviceDiscovery,client,providers);
        } finally {
            for (ServiceProvider<InstanceDetails> value : providers.values()) {
                CloseableUtils.closeQuietly(value);
            }
            CloseableUtils.closeQuietly(server);
            CloseableUtils.closeQuietly(serviceDiscovery);
            CloseableUtils.closeQuietly(client);
        }
    }

    private static void processCommands(ServiceDiscovery<InstanceDetails> serviceDiscovery, CuratorFramework client, HashMap<String, ServiceProvider<InstanceDetails>> providers) throws Exception {
        printHelper();
        ArrayList<ExampleServer> servers = Lists.newArrayList();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done){
            System.out.println(">");
            String line = in.readLine();
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
            } else if (operation.equals("add")) {
                addInstance(args, client, command, servers);
            } else if (operation.equals("delete")) {
                deleteInstance(args, command, servers);
            } else if (operation.equals("random")) {
                listRandomInstance(args, serviceDiscovery, providers, command);
            } else if (operation.equals("list")) {
                listInstances(serviceDiscovery);
            }

        }


    }

    /**
    * 展示所有实例及描述
    * @param serviceDiscovery
    * @return void
    * @exception
    **/
    private static void listInstances(ServiceDiscovery<InstanceDetails> serviceDiscovery) throws Exception {
        try {
            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            System.out.println(serviceNames.size() + "type(s)");
            for (String serviceName : serviceNames) {
                Collection<ServiceInstance<InstanceDetails>> instances = serviceDiscovery.queryForInstances(serviceName);
                System.out.println(serviceName);
                for (ServiceInstance<InstanceDetails> instance : instances) {
                    outputInstance(instance);
                }
            }
        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
        }
    }

    /**
    * 展示具体实例中的随机实例内容
    * @param args
    * @param serviceDiscovery
    * @param providers
    * @param command
    * @return void
    * @exception
    **/
    private static void listRandomInstance(String[] args, ServiceDiscovery<InstanceDetails> serviceDiscovery, HashMap<String, ServiceProvider<InstanceDetails>> providers, String command) throws Exception {
        if (args.length != 1){
            System.out.println("syntax error (excepted random <name>:)"+command);
            return;
        }
        String serviceName = args[0];
        ServiceProvider<InstanceDetails> provider = providers.get(serviceName);
        if (provider == null){
            provider = serviceDiscovery.serviceProviderBuilder().serviceName(serviceName)
                    .providerStrategy(new RandomStrategy<InstanceDetails>()).build();
            providers.put(serviceName,provider);
            provider.start();
            Thread.sleep(2500);
        }
        ServiceInstance<InstanceDetails> instance = provider.getInstance();
        if (instance == null){
            System.err.println("No instances named:"+serviceName);
        }else {
            outputInstance(instance);
        }
    }

    private static void outputInstance(ServiceInstance<InstanceDetails> instance) {
        System.out.println("\t"+instance.getPayload().getDescription()+":"+instance.buildUriSpec());
    }

    /**
    * 删除存在的实例
    * @param args
    * @param command
    * @param servers
    * @return void
    * @exception
    **/
    private static void deleteInstance(String[] args, String command, ArrayList<ExampleServer> servers) {
        if (args.length != 1){
            System.err.println("syntax error (excepted delete <name>)"+command);
            return;
        }
        final String serviceName = args[0];
        ExampleServer exampleServer = Iterables.find(servers, new Predicate<ExampleServer>() {
            @Override
            public boolean apply(@Nullable ExampleServer input) {
                return input.getServiceInstance().getName().endsWith(serviceName);
            }
        }, null);
        if (exampleServer == null){
            System.err.println("No Servers found named:"+ serviceName);
            return;
        }
        servers.remove(exampleServer);
        CloseableUtils.closeQuietly(exampleServer);
        System.out.println("Remove a random instance of "+ serviceName);
    }

    /**
    * 添加实例内容及描述
    * 利用ExampleServer调用，
    * @param args
    * @param client
    * @param command
    * @param servers
    * @return void
    * @exception
    **/
    private static void addInstance(String[] args, CuratorFramework client, String command, ArrayList<ExampleServer> servers) throws Exception {
        if (args.length != 2){
            System.err.println("syntax error excepted add <name> <description>:"+command);
            return;
        }
        StringBuilder description = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 1){
                description.append(" ");
            }
            description.append(args[i]);
        }
        String serverName = args[0];
        ExampleServer exampleServer = new ExampleServer(client, PATH, serverName, description.toString());
        servers.add(exampleServer);
        exampleServer.start();
        System.out.println(serverName + "added");
    }

    private static void printHelper() {
        System.out.println("An example of using the ServiceDiscovery APIs. This example is driven by entering commands at the prompt:\n");
        System.out.println("add <name> <description>: Adds a mock service with the given name and description");
        System.out.println("delete <name>: Deletes one of the mock services with the given name");
        System.out.println("list: Lists all the currently registered services");
        System.out.println("random <name>: Lists a random instance of the service with the given name");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}
