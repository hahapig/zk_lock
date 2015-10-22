package com.wangying.asset;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.wangying.asset.framework.lock.DistributedZKLock;

/**
 * Hello world!
 *
 */
public class App 
{
	
	public static int COUNTER = 0;
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        singleNode();
//        simulateMutiNode();
    }
    
    public static String singleNode(){
    	try {
			DistributedZKLock distributedZKLock = new DistributedZKLock("127.0.0.1:2181",true);
			localPreferTest(distributedZKLock);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return "sss";
    }
    
    public static void localPreferTest(final DistributedZKLock distributedZKLock ){
    	final long starTime = System.currentTimeMillis();
    	final CountDownLatch countDownLatch = new CountDownLatch(40);
    	for (int i = 0; i < 40; i++) {
			(new Thread(){
				public void run() {
					try {
						countDownLatch.await();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					distributedZKLock.lock();
					System.out.println("Thread _name："+this.getName()+"；已获得计数器:"+COUNTER++);
					distributedZKLock.unlock();
				}
			}).start();
			countDownLatch.countDown();
		}
    	Runtime.getRuntime().addShutdownHook(new Thread(){
    			public void run(){
    				System.out.println("耗时："+(System.currentTimeMillis()-starTime));
    			}
    	}
    			);
    }
    
    public static void simulateMutiNode(){
    	final long starTime = System.currentTimeMillis();
    	final int nodeNum = 20;
    	final CountDownLatch countDownLatch = new CountDownLatch(5);
    	for (int i = 0; i < nodeNum; i++) {
    		(new Thread(){
				public void run() {
					try {
						countDownLatch.await();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					DistributedZKLock distributedZKLock = null;;
					try {
						distributedZKLock = new DistributedZKLock("127.0.0.1:2181",true);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					distributedZKLock.lock();
					System.out.println("Thread _name："+this.getName()+"；已获得计数器:"+COUNTER+++"锁ID"+DistributedZKLock.getLockId(distributedZKLock));
					distributedZKLock.unlock();
				}
			}).start();
			countDownLatch.countDown();
		}
    	
    	Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run(){
				System.out.println("耗时："+(System.currentTimeMillis()-starTime));
			}
	}
			);
    	
    }
}
