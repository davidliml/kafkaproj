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
				// �յ�watch֪ͨ��Ļص�����
				System.out.println("�¼�����" + event.getType() + "��·��" + event.getPath());

				// ��Ϊ������ֻ�����һ�Σ���������һֱ����,��ֻ����"/"Ŀ¼
				try {
					zk.getChildren("/", true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});

		// ����һ���ڵ㣬���ش����õ�·�� �����ϴ������ݿ���Ϊ�������ͣ���Ҫת����byte[]
		// ����1 ·��������2 ���ݣ�����3 Ȩ�ޣ�����4 ����
		String znodePath = zk.create("/chroot", "hello zookeeper".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("���ص�·�� Ϊ��" + znodePath);

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
			System.out.println("������");
		} else {
			System.out.println("����");
		}
		Thread.sleep(100000);
		System.out.println("over");

		// ����create
		// create();

		// ��ȡ�ӽڵ�
		// getChildren();

		// �ж��Ƿ����
		// isExist();

		// ��ȡznode����
		// getData();

		// ɾ��
		// delete();

		// �޸�
		// setData();
	}

	/**
	 * ��ȡzookeeperʵ��
	 * 
	 * @return
	 * @throws Exception
	 */
	public static ZooKeeper getZookeeper() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// �յ�watch֪ͨ��Ļص�����
				System.out.println("�¼�����" + event.getType() + "��·��" + event.getPath());

				// ��Ϊ������ֻ�����һ�Σ���������һֱ����,��ֻ����"/"Ŀ¼
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
	 * ��������
	 * 
	 * @throws Exception
	 */
	public static void create() throws Exception {
		ZooKeeper zk = getZookeeper();
		// ����һ���ڵ㣬���ش����õ�·�� �����ϴ������ݿ���Ϊ�������ͣ���Ҫת����byte[]
		// ����1 ·��������2 ���ݣ�����3 Ȩ�ޣ�����4 ����
		String znodePath = zk.create("/chroot", "hello zookeeper".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("���ص�·�� Ϊ��" + znodePath);
	}

	/**
	 * �ж�znode�Ƿ����
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

		// TODO �Զ����ɵķ������

				);

		if (exists == null) {
			System.out.println("������");
		} else {
			System.out.println("����");
		}
		Thread.sleep(100000);
		System.out.println("over");
		// zk.close();
	}

	/**
	 * ��ȡ�ӽڵ�
	 * 
	 * @throws Exception
	 */
	public static void getChildren() throws Exception {
		ZooKeeper zk = getZookeeper();
		// ��ȡ�ӽڵ�
		List<String> children = zk.getChildren("/", true);
		for (String string : children) {
			System.out.println("�ӽڵ�:" + string);
		}
		// ���������Լ�����,��ֻ����"/"Ŀ¼
		Thread.sleep(Long.MAX_VALUE);
	}

	/**
	 * ��ȡznode����
	 * 
	 * @throws Exception
	 */
	public static void getData() throws Exception {
		ZooKeeper zk = getZookeeper();
		byte[] data = zk.getData("/lijie/test", false, new Stat());
		System.out.println(new String(data));
	}

	/**
	 * ɾ������
	 * 
	 * @throws Exception
	 */
	public static void delete() throws Exception {
		ZooKeeper zk = getZookeeper();
		// �ڶ�������Ϊversion��-1��ʾɾ�����а汾
		// ����֧��ɾ���Ľڵ����滹���ӽڵ㣬ֻ�ܵݹ�ɾ��
		zk.delete("/hehe", -1);
	}

	/**
	 * �޸�znode��ֵ
	 * 
	 * @throws Exception
	 */
	public static void setData() throws Exception {
		ZooKeeper zk = getZookeeper();

		// �޸�znode��ֵ
		zk.setData("/lijie", "modify data".getBytes(), -1);

		// �����Ƿ��޸ĳɹ�
		System.out.println(new String(zk.getData("/lijie", false, null)));

	}

}
