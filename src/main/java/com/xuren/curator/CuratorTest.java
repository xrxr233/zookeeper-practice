package com.xuren.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CuratorTest {
    /* 客户端 */
    private CuratorFramework client = null;

    @BeforeEach
    public void testConnect() {
        /*
         * Create a new client
         *
         * @param connectString       zk服务端的IP和端口号 "192.168.161.132:2181"
         * @param sessionTimeoutMs    会话超时时间，单位为毫秒
         * @param connectionTimeoutMs 连接超时时间，单位为毫秒
         * @param retryPolicy         重试策略
         * @return client
         */
//        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.161.132:2181", 60 * 1000,
//                15 * 1000, new ExponentialBackoffRetry(3000, 10));

        client = CuratorFrameworkFactory.builder().connectString("192.168.161.132:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(3000, 10))
                .namespace("xuren")
                .build();

        //开启连接
        client.start();
    }

    @AfterEach
    public void close() {
        if(client != null) {
            client.close();
        }
    }

    /**
     * 创建节点
     */
    @Test
    public void testCreate() throws Exception {
//        String path = client.create().forPath("/app1");
//        System.out.println(path);

//        String path = client.create().forPath("/app2", "Hello".getBytes());
//        System.out.println(path);

        //临时节点，会话关闭会被删除
//        String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
//        System.out.println(path);
//        Thread.sleep(10 * 1000);


    }
}
