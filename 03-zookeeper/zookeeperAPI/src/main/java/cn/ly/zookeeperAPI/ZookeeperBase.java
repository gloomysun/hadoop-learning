package cn.ly.zookeeperAPI;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

public class ZookeeperBase {
    /**
     * zookeeper地址
     */
    static final String CONNECT_ADDRESS = "192.168.0.200:2181,192.168.0.201:2181,192.168.0.202:2181";
    /**
     * session超时时间
     */
    static final int SESSION_OUTTIME = 2000;  //ms
    /**
     * 信号量，阻塞程序执行，用于等待zookeeper连接成功
     */
    static final CountDownLatch LATCH = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(CONNECT_ADDRESS, SESSION_OUTTIME, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("============" + watchedEvent.toString() + "============");
                //获取时间的状态
                Event.KeeperState state = watchedEvent.getState();
                Event.EventType eventType = watchedEvent.getType();
                //如果是建立连接
                if (Event.KeeperState.SyncConnected == state) {
                    if (Event.EventType.None == eventType) {
                        //如果建立连接成功，发送信号量，让后续阻塞程序向下执行
                        LATCH.countDown();
                        System.out.println("zk 建立连接");
                    }
                }

            }
        });
        LATCH.await();
        System.out.println("执行后续操作");

        //1.创建父节点
//        zooKeeper.create("/testRoot", "testRoot".getBytes(),
//                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //2.创建子节点,使用EPHEMERAL创建临时节点，程序执行完后删除
        zooKeeper.create("/testRoot/testChild", "testChild".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        //3.获取节点信息
        byte[] data = zooKeeper.getData("/testRoot", true, null);
        System.out.println("testRoot节点上的数据:"+new String(data));
        System.out.println("testRoot子节点:"+zooKeeper.getChildren("/testRoot", false, null));

        //4.修改节点的值，-1表示跳过检查，其他正数表示如果传入的版本号与当前版本号不一致，则修改不成功，删除是同样的道理。
        zooKeeper.setData("/testRoot", "testRoot2".getBytes(), -1);
        byte[] newdata = zooKeeper.getData("/testRoot", false, null);
        System.out.println("testRoot节点上修改后的数据:"+new String(newdata));
        //5.判断节点是否存在
        System.out.println("/testRoot/testChild是否存在:"+zooKeeper.exists("/testRoot/testChild",false));
        //6.删除节点
        zooKeeper.delete("/testRoot/testChild",-1);
        System.out.println("/testRoot/testChild是否存在:"+zooKeeper.exists("/testRoot/testChild",false));

        zooKeeper.close();
    }
}
