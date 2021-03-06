package com.zhuai66.zookeeper.demo;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Create {
	// private static final String connectString =
	// "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static final String connectString = "192.168.100.183:2181";

	private static final int sessionTimeout = 2000;

	private static ZooKeeper zk = null;

	public static void main(String[] args) throws Exception {

		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 收到watch通知后的回调函数
				System.out.println("事件类型" + event.getType() + "，路径" + event.getPath());

				// 因为监听器只会监听一次，这样可以一直监听,且只监听"/"目录
				try {
					zk.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		// 创建一个节点，返回创建好的路径 ，且上传的数据可以为任意类型，需要转换成byte[]
		// 参数1 路径，参数2 内容，参数3 权限，参数4 类型
		String znodePath = zk.create("/chroot", "hello zookeeper".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("返回的路径 为：" + znodePath);

		Stat exists = zk.exists("/chroot", new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + "|" + event.getType().name());
				try {
					
					zk.exists("/chroot", this);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}


				);

		if (exists == null) {
			System.out.println("不存在");
		} else {
			System.out.println("存在");
		}
		Thread.sleep(100000);
		System.out.println("over");

		// 调用create
		// create();

		// 获取子节点
		// getChildren();

		// 判断是否存在
		// isExist();

		// 获取znode数据
		// getData();

		// 删除
		// delete();

		// 修改
		// setData();
	}

	/**
	 * 获取zookeeper实例
	 * 
	 * @return
	 * @throws Exception
	 */
	public static ZooKeeper getZookeeper() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// 收到watch通知后的回调函数
				System.out.println("事件类型" + event.getType() + "，路径" + event.getPath());

				// 因为监听器只会监听一次，这样可以一直监听,且只监听"/"目录
				try {
					zk.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		return zk;
	}

	/**
	 * 创建数据
	 * 
	 * @throws Exception
	 */
	public static void create() throws Exception {
		ZooKeeper zk = getZookeeper();
		// 创建一个节点，返回创建好的路径 ，且上传的数据可以为任意类型，需要转换成byte[]
		// 参数1 路径，参数2 内容，参数3 权限，参数4 类型
		String znodePath = zk.create("/chroot", "hello zookeeper".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("返回的路径 为：" + znodePath);
	}

	/**
	 * 判断znode是否存在
	 * 
	 * @throws Exception
	 */
	public static void isExist() throws Exception {
		final ZooKeeper zk = getZookeeper();
		Stat exists = zk.exists("/chroot", new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath() + "|" + event.getType().name());
				try {
					zk.exists("/chroot", this);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		// TODO 自动生成的方法存根

				);

		if (exists == null) {
			System.out.println("不存在");
		} else {
			System.out.println("存在");
		}
		Thread.sleep(100000);
		System.out.println("over");
		// zk.close();
	}

	/**
	 * 获取子节点
	 * 
	 * @throws Exception
	 */
	public static void getChildren() throws Exception {
		ZooKeeper zk = getZookeeper();
		// 获取子节点
		List<String> children = zk.getChildren("/", true);
		for (String string : children) {
			System.out.println("子节点:" + string);
		}
		// 阻塞，测试监听器,且只监听"/"目录
		Thread.sleep(Long.MAX_VALUE);
	}

	/**
	 * 获取znode数据
	 * 
	 * @throws Exception
	 */
	public static void getData() throws Exception {
		ZooKeeper zk = getZookeeper();
		byte[] data = zk.getData("/lijie/test", false, new Stat());
		System.out.println(new String(data));
	}

	/**
	 * 删除数据
	 * 
	 * @throws Exception
	 */
	public static void delete() throws Exception {
		ZooKeeper zk = getZookeeper();
		// 第二个参数为version，-1表示删除所有版本
		// 它不支持删除的节点下面还有子节点，只能递归删除
		zk.delete("/hehe", -1);
	}

	/**
	 * 修改znode的值
	 * 
	 * @throws Exception
	 */
	public static void setData() throws Exception {
		ZooKeeper zk = getZookeeper();

		// 修改znode的值
		zk.setData("/lijie", "modify data".getBytes(), -1);

		// 测试是否修改成功
		System.out.println(new String(zk.getData("/lijie", false, null)));

	}

}
