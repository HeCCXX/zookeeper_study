package com.distributionLock.redisLock;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName LockTest
 * @Description redis单节点分布式锁
 * 需要导入pom依赖或相应依赖包
 *          <dependency>
 *             <groupId>redis.clients</groupId>
 *             <artifactId>jedis</artifactId>
 *             <version>2.9.0</version>
 *         </dependency>
 * @Author 贺楚翔
 * @Date 2020-05-09 15:09
 * @Version 1.0
 **/
public class LockTest {

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";

    private static final Long RELEASE_SUCCESS = 1L;

    private static String host = "192.168.145.110";
    private static int port = 6379;
    private static String password = "taredis";
    private Jedis jedis ;
    private static String lock = "mylock";

    public LockTest() {
        jedis = new Jedis(host,port);
        jedis.auth(password);
        jedis.select(1);
    }
//    static {
//        jedis = new Jedis(host,port);
//        jedis.auth(password);
//        jedis.select(0);
//    }

    @Test
    public void test(){
        jedis.set("mylock","hcx",SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME,5000);
    }

    public void lock(String name, String id){
        while (true){
            String result = jedis.set(name, id, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, 10000);
            if (LOCK_SUCCESS.equals(result)){
                System.out.println(Thread.currentThread().getName()+"get  lock...");
                return;
            }else {
                System.out.println(Thread.currentThread().getName()+"waiting for get lock ...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void release(String key, String requestid){
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(key), Collections.singletonList(requestid));
        if (RELEASE_SUCCESS.equals(result)){
            System.out.println(Thread.currentThread().getName()+"release lock!!!!!");
        }else {
            System.out.println("解锁失败：解锁用户："+requestid+"，锁为："+key);
        }
    }

    public static void main(String[] args) {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        ThreadPoolExecutor.DiscardPolicy handler = new ThreadPoolExecutor.DiscardPolicy();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS, queue, handler);
        for (int i = 0; i < 4; i++) {
            threadPoolExecutor.execute(() -> {
                LockTest lockTest = new LockTest();
                lockTest.lock(lock,Thread.currentThread().getName());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lockTest.release(lock,Thread.currentThread().getName());
            });
        }
        threadPoolExecutor.shutdown();
    }
}
