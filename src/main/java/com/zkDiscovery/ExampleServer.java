package com.zkDiscovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.io.Closeable;
import java.io.IOException;

/**
 * @ClassName ExampleServer
 * @Description 相当于分布式环境中的服务应用，应用启动调用start，关闭调用close
 * @Author 贺楚翔
 * @Date 2020-06-08 10:35
 * @Version 1.0
 **/
public class ExampleServer implements Closeable {
    private final ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private final ServiceInstance<InstanceDetails> serviceInstance;

    public ExampleServer(CuratorFramework client,String path,String serviceName,String discription) throws Exception {
        UriSpec url = new UriSpec("{scheme}://foo.com:{port}");
        serviceInstance = ServiceInstance.<InstanceDetails>builder().name(serviceName).payload(new InstanceDetails(discription))
                .port((int) (65535 * Math.random()))
                .uriSpec(url).build();

        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<>(InstanceDetails.class);
        serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client).basePath(path).serializer(serializer)
                .thisInstance(serviceInstance).build();
    }

    public ServiceInstance<InstanceDetails> getServiceInstance() {
        return serviceInstance;
    }

    public void start() throws Exception {
        serviceDiscovery.start();
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(serviceDiscovery);
    }
}
