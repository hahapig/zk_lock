package com.wangying.asset.framework.lock;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * 实现分布式锁<br/>
 * 保留线程中断特性<br/>
 * 保留条件锁特性<br/>
 * 同一进程内多线程竞争，进行本地排队<br/>
 * 支持本地排队优先执行策略<br/>
 * 
 * @author YangTao
 * @since 2015-10-21
 * 
 */
public class DistributedZKLock implements Lock {

	private static int SESSION_TIMEOUT = 10 * 1000;// zk会话超时时间
	private static int CONNECTION_TIMEOUT = 5 * 1000;// zk连接超时时间
	private static boolean DEFAULT_LOCALPREFER = true;// 是否允许本地排队的线程优先执行
	private static String LOCK_ROOT_PATH = "/"
			+ DistributedZKLock.class.getName();
	private static AtomicInteger LOCK_COUNT = new AtomicInteger(0);
	private static final int RETRY_COUNT = 3;
	private static final byte[] data = { 0x12, 0x34 };
	
	private final boolean localPrefer;
	private volatile int lockId = -1;
	private final ZooKeeper zkClient;
	private volatile boolean nonCancel = true;// 只有当这个变量值为true的时候，才能完全确保可使用localPrefer的模式
	private ThreadLocal<Long> timeOut = new ThreadLocal<Long>();
	// 远程资源相关---不会并发访问，只可能单线程访问
	private volatile String id;
	private volatile LockNode idName;
	private volatile boolean hasRemoteSource = false;

	private final Sync sync;

	public DistributedZKLock(String zkHost) throws IOException,
			KeeperException, InterruptedException {
		this(zkHost, DEFAULT_LOCALPREFER);
	}

	public DistributedZKLock(String zkHost, boolean localPrefer)
			throws IOException, KeeperException, InterruptedException {
		this.localPrefer = localPrefer;
		final CountDownLatch init = new CountDownLatch(1);
		this.zkClient = new ZooKeeper(zkHost, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				init.countDown();
			}
		});
		if (!init.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
			throw new IllegalStateException(
					"waiting time out after 5s. DistributedZKLock init failed");
		}
		sync = new Sync();
		onConnection();
		lockId = LOCK_COUNT.getAndIncrement();
	}

	/*
	 * 注意：
	 * 
	 * •使用EPHEMERAL会引出一个风险：在非正常情况下，网络延迟比较大会出现session
	 * timeout，zookeeper就会认为该client已关闭，从而销毁其id标示，竞争资源的下一个id就可以获取锁。
	 * 这时可能会有两个process同时拿到锁在跑任务，所以设置好session timeout很重要。
	 * •同样使用PERSISTENT同样会存在一个死锁的风险，进程异常退出后，对应的竞争资源id一直没有删除，下一个id一直无法获取到锁对象。
	 */

	// 远程资源处理

	private void onConnection() throws KeeperException, InterruptedException {
		if (null == zkClient.exists(LOCK_ROOT_PATH, false)) {
			zkClient.create(LOCK_ROOT_PATH, "".getBytes(),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	// 发起远程排他获取，如果获取不到需要阻塞
	private void acquireRemoteResource(){
		CountDownLatch latch = new CountDownLatch(1);// 用于异步监听线程和主线程间的交互
		for (int i = 0; i < RETRY_COUNT; i++) {
			try {
				if (doAcquireRemoteResource(latch)) {
					break;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		try {
			if(timeOut.get()!=null){
				 if(!latch.await(timeOut.get(),TimeUnit.NANOSECONDS)){
					 throw new InterruptedException();
				 }
			}else{
				latch.await();
			}
		} catch (InterruptedException e) {
			releaseRemoteResource();
			throw new RuntimeException(e);
 		}
	}

	private boolean doAcquireRemoteResource(final CountDownLatch latch)
			throws KeeperException, InterruptedException {
		do {
			if (id == null) {// 构建当前lock的唯一标识
				long sessionId = zkClient.getSessionId();
				String prefix = "x-" + sessionId + "-";
				// 如果第一次，则创建一个节点
				String path = zkClient.create(LOCK_ROOT_PATH + "/" + prefix,
						data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
				int index = path.lastIndexOf("/");
				id = StringUtils.substring(path, index + 1);
				idName = new LockNode(id);
			}

			if (id != null) {
				List<String> names = zkClient
						.getChildren(LOCK_ROOT_PATH, false);
				if (names.isEmpty()) {
					nulledOut();
				} else {
					// 对节点进行排序
					SortedSet<LockNode> sortedNames = new TreeSet<LockNode>();
					for (String name : names) {
						sortedNames.add(new LockNode(name));
					}

					if (!sortedNames.contains(idName)) {
						nulledOut();
						continue;
					}
					String ownerId;
					String lastChildId;
					// 将第一个节点做为ownerId
					ownerId = sortedNames.first().getName();
					if (id.equals(ownerId)) {
						latch.countDown();
						return true;
					}

					SortedSet<LockNode> lessThanMe = sortedNames
							.headSet(idName);
					if (!lessThanMe.isEmpty()) {
						// 关注一下排队在自己之前的最近的一个节点
						LockNode lastChildName = lessThanMe.last();
						lastChildId = lastChildName.getName();
						// 异步watcher处理
						Stat stat = zkClient.exists(LOCK_ROOT_PATH + "/"
								+ lastChildId, new Watcher() {
							public void process(WatchedEvent event) {
								latch.countDown();
							}
						});

						if (stat == null) {
							doAcquireRemoteResource(latch);// 如果节点不存在，需要自己重新触发一下，watcher不会被挂上去
						}
					} else {
						if (id.equals(ownerId)) {
							latch.countDown();
						} else {
							nulledOut();
						}
					}
				}
			}
		} while (id == null);
		return false;
	}

	private void nulledOut() {
		try {
			if (id != null) {
				zkClient.delete(LOCK_ROOT_PATH + "/" + id, -1);
			} else {
				// do nothing
			}
		} catch (Exception e) {
		} finally {
			idName = null;
			hasRemoteSource = false;
			id = null;
		}

	}

	// 释放远程资源
	private void releaseRemoteResource() {
		nulledOut();
	}

	// 获取zktoken
	private boolean hasRemoteResource() {
		return hasRemoteSource;
	}

	// 设置zktoken
	private void remainRemoteResource() {
		hasRemoteSource = true;
	}

	// 锁实现方法

	public void lock() {
		sync.acquire(1);
	}

	public void lockInterruptibly() throws InterruptedException {
		nonCancel = false;
		sync.acquireInterruptibly(1);
	}

	public boolean tryLock() {
		return sync.tryAcquire(1);
	}
	//慎用此方法，虽然已经实现，可能因为超时误删节点，造成并发执行、
	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		timeOut.set(unit.toNanos(time));
		return sync
				.tryAcquireNanos(1, unit.toNanos(time));
	}

	public void unlock() {
		sync.release(1);
	}

	public Condition newCondition() {
		nonCancel = false;
		return sync.newCondition();
	}

	// 同步支持类

	final class Sync extends AbstractQueuedSynchronizer {

		private static final long serialVersionUID = 1L;

		protected final boolean tryAcquire(int acquires) {
			final Thread current = Thread.currentThread();
			int c = getState();
			if (c == 0) {
				if (compareAndSetState(0, acquires)) {
					setExclusiveOwnerThread(current);
					// 本虚拟机内抢占成功，需要向zk发起独占请求
					if (hasRemoteResource()) {
						// 什么也不做
					} else {
						acquireRemoteResource();
					}
					return true;
				}
			} else if (current == getExclusiveOwnerThread()) {// 提供重入机制
				return true;
			}
			return false;
		}

		// tryRelease的时候是排他获取状态，可以无条件进行修改state
		protected final boolean tryRelease(int releases) {
			int c = getState() - releases;
			if (Thread.currentThread() != getExclusiveOwnerThread())
				throw new IllegalMonitorStateException();
			boolean free = false;
			if (c == 0) {
				free = true;
				setExclusiveOwnerThread(null);
			} else {// 出现这异常，真实日了鬼了
				throw new IllegalStateException();
			}
			// 因为前面的tryAcquire的原因，所以必须在重新设置状态之前，将已获取的远程资源保留。
			if (DistributedZKLock.this.localPrefer
					&& DistributedZKLock.this.nonCancel && hasQueuedThreads()) {
				remainRemoteResource();
			} else {
				// 释放远程资源
				releaseRemoteResource();
			}
			setState(c);
			return free;
		}

		final ConditionObject newCondition() {
			return new ConditionObject();
		}
	}

	// 其他工具方法

	public static int countLock() {
		return LOCK_COUNT.get();
	}

	public static int getLockId(DistributedZKLock lock) {
		return lock.lockId;
	}

}
