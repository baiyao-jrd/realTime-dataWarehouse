package com.atguigu.gmall.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Felix
 * @date 2022/9/16
 * 获取线程池的工具类
 * 双重校验锁解决单例设计模式懒汉式线程安全问题
 */
public class ThreadPoolUtil {
    private static volatile ThreadPoolExecutor poolExecutor;
    /*static {
        poolExecutor = new ThreadPoolExecutor(
            4,20,300, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
    }*/

    public static ThreadPoolExecutor getInstance(){
        if(poolExecutor == null){
            synchronized(ThreadPoolUtil.class){
                if(poolExecutor == null){
                    System.out.println("~~开辟线程池~~");
                    poolExecutor = new ThreadPoolExecutor(
                        4,20,300, TimeUnit.SECONDS,
                        new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }

        return poolExecutor;
    }
}
