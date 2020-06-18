package com.transaction;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.util.Collection;

/**
 * @ClassName TransactionTest
 * @Description zookeeper 事务，高版本inTransaction已过时
 * @Author 贺楚翔
 * @Date 2020-06-18 10:16
 * @Version 1.0
 **/
public class TransactionTest {
    public static void main(String[] args) throws Exception {
        final TestingServer server = new TestingServer();

        final CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(2000, 3));
        client.start();
        client.create().forPath("/example","parent".getBytes());
//        transaction(client);
        final CuratorTransaction curatorTransaction = startTransaction(client);
        final CuratorTransactionFinal curatorTransactionFinal = addCreateToTransaction(curatorTransaction);
        final CuratorTransactionFinal curatorTransactionFinal1 = addDeleteToTransaction(curatorTransaction);
//        commitTransaction(curatorTransactionFinal);
        commitTransaction(curatorTransactionFinal1);

        CloseableUtils.closeQuietly(server);
        CloseableUtils.closeQuietly(client);
    }

    /**
    * 流式api，使用and连接操作，使各个操作称为一个事务，要么成功，要么失败，返回的事务结果可以输出结果
    * @param client
    * @return java.util.Collection<org.apache.curator.framework.api.transaction.CuratorTransactionResult>
    * @exception
    **/
    public static Collection<CuratorTransactionResult> transaction(CuratorFramework client) throws Exception {
        final Collection<CuratorTransactionResult> results = client.inTransaction().create().forPath("/example/path", "data".getBytes())
                .and().setData().forPath("/example/path", "another data".getBytes())
                .and().create().forPath("/example/path2","hcx".getBytes())
                .and().commit();
        for (CuratorTransactionResult result : results) {
            System.out.println(result.getForPath() + " - " + result.getType());
        }
        return results;
    }

    /**
    * 以下的api通过各个方法分开调用，最终还是实现流式接口的事务操作的结果
    * @param client
    * @return org.apache.curator.framework.api.transaction.CuratorTransaction
    * @exception
    **/
    public static CuratorTransaction startTransaction(CuratorFramework client){
        return client.inTransaction();
    }

    public static CuratorTransactionFinal addCreateToTransaction(CuratorTransaction transaction) throws Exception {
        return transaction.create().forPath("/example/path","data".getBytes()).and();
    }

    public static CuratorTransactionFinal addDeleteToTransaction(CuratorTransaction transaction) throws Exception {
        return transaction.delete().forPath("/example/path").and();
    }

    public static void commitTransaction(CuratorTransactionFinal transactionFinal) throws Exception {
        final Collection<CuratorTransactionResult> results = transactionFinal.commit();
        for (CuratorTransactionResult result : results) {
            System.out.println(result.getForPath() + " - " + result.getType());
        }
    }
}
