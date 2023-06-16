package com.xuren.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class Ticket12306 implements Runnable {
    /* 数据库的票数 */
    private int tickets = 10;

    private InterProcessLock lock;

    public Ticket12306() {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("192.168.161.132:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(new ExponentialBackoffRetry(3000, 10))
                .namespace("xuren")  //每次执行操作都是以/xuren为根节点
                .build();

        //开启连接
        client.start();
        lock = new InterProcessMutex(client, "/lock");
    }

    @Override
    public void run() {
        while(true) {
            //加锁
            try {
                lock.acquire(3, TimeUnit.SECONDS);
                if(tickets > 0) {
                    System.out.println(Thread.currentThread() + ":" + tickets);
                    tickets--;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                //释放锁
                try {
                    lock.release();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
