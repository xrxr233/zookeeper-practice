package com.xuren.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

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
                .namespace("xuren")  //每次执行操作都是以/xuren为根节点
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
        String path1 = client.create().forPath("/app1");
        System.out.println(path1);

        String path2 = client.create().forPath("/app2", "Hello".getBytes());
        System.out.println(path2);

        //临时节点，会话关闭会被删除
        String path3 = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
        System.out.println(path3);
        Thread.sleep(10 * 1000);

    }

    /**
     * 查询节点
     */
    @Test
    public void testGet() throws Exception {
        //查询节点的数据 get
        byte[] data = client.getData().forPath("/app1");
        System.out.println(new String(data));

        //查询子节点 ls
        List<String> path = client.getChildren().forPath("/");
        System.out.println(path);

        //查询节点的状态信息 ls -s
        Stat status = new Stat();
        client.getData().storingStatIn(status).forPath("/app1");
        System.out.println(status);
    }

    /**
     * 设置节点数据
     * @throws Exception
     */
    @Test
    public void testSetForVersion() throws Exception {
        //乐观锁方式（版本号）修改节点数据
        Stat status = new Stat();
        client.getData().storingStatIn(status).forPath("/app1");
        client.setData().withVersion(status.getVersion()).forPath("/app1", "HeHe".getBytes());
    }

    /**
     * 删除节点
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        //保证删除成功，因为有可能因为网络原因导致删除失败，所以会多次重试
        client.delete().guaranteed().forPath("/app1");
    }
}
