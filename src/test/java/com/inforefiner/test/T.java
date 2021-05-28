package com.inforefiner.test;

import com.google.common.util.concurrent.RateLimiter;

public class T{

    public static void main(String[] args) {
        testNoRateLimiter();
        testWithRateLimiter();
    }

    public static void testNoRateLimiter() {
        Long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            System.out.println("call execute.." + i); // 未限流总耗时:3
        }
        Long end = System.currentTimeMillis();
        System.out.println("未限流总耗时:" + (end - start)); // 进行限流后耗时:9912
    }

    public static void testWithRateLimiter() {
        Long start = System.currentTimeMillis();
        // 每秒不超过10个任务被提交
        RateLimiter limiter = RateLimiter.create(10.0);
        for (int i = 0; i < 100; i++) {
            // 请求RateLimiter, 超过permits会被阻塞，然后等待获取令牌
            limiter.acquire();
            System.out.println("call execute.." + i);
        }
        Long end = System.currentTimeMillis();
        System.out.println("进行限流后耗时:" + (end - start));
    }
}
