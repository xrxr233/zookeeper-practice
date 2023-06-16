package com.xuren.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CuratorWatcherTest {
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
     * 演示NodeCache：给指定一个节点注册监听器，即监听某一个节点
     */
    @Test
    public void testNodeCache() throws Exception {
        //创建NodeCache对象
        NodeCache nodeCache = new NodeCache(client, "/app1");

        //注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了");
                //获取节点当前数据
                byte[] data = nodeCache.getCurrentData().getData();
                System.out.println(new String(data));
            }
        });

        //开启监听，如果设置为true，则开启监听时加载缓存数据
        nodeCache.start(true);

        //等待事件发生
        while(true) {

        }
    }

    /**
     * 演示PathChildrenCache：监听某个节点的所有子节点，不包括当前节点
     * @throws Exception
     */
    @Test
    public void testChildrenCache() throws Exception {
        //创建NodeCache对象
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app2", true);

        //绑定监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点变化了");
                System.out.println(pathChildrenCacheEvent);
                //子节点数据变更时，才打印数据
                if(pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    System.out.println(new String(pathChildrenCacheEvent.getData().getData()));
                }
            }
        });

        //开启监听
        pathChildrenCache.start();


        //等待事件发生
        while(true) {

        }
    }

    /**
     * 演示TreeCache：监听某个节点及其子节点
     */
    @Test
    public void testTreeCache() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/");

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("节点变化了");
                System.out.println(treeCacheEvent);
            }
        });

        treeCache.start();

        while(true) {

        }
    }


}
