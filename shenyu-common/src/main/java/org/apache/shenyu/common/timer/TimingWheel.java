/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.common.timer;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 时间轮
 * TimingWheel .
 * This is a Hierarchical wheel timer implementation.
 * @see <a href="https://learn.lianglianglee.com/%E4%B8%93%E6%A0%8F/Kafka%E6%A0%B8%E5%BF%83%E6%BA%90%E7%A0%81%E8%A7%A3%E8%AF%BB/19%20TimingWheel%EF%BC%9A%E6%8E%A2%E7%A9%B6Kafka%E5%AE%9A%E6%97%B6%E5%99%A8%E8%83%8C%E5%90%8E%E7%9A%84%E9%AB%98%E6%95%88%E6%97%B6%E9%97%B4%E8%BD%AE%E7%AE%97%E6%B3%95.md">Kafka时间轮算法</a>
 */
class TimingWheel {
    /**滴答一次的时长，类似于手表的例子中向前推进一格的时间。对于秒针而言，tickMs就是1秒。同理，分针是1分，时针是1小时**/
    private final Long tickMs;

    /**每一层时间轮上的Bucket数量**/
    private final Integer wheelSize;

    /**每一层上的总定时任务数**/
    private final AtomicInteger taskCounter;

    /**将所有Bucket按照过期时间排序的延迟队列。随着时间不断向前推进，需要依靠这个队列获取那些已过期的Bucket，并清除它们**/
    private final DelayQueue<TimerTaskList> queue;

    /**
     * 这层时间轮总时长，等于滴答时长乘以wheelSize。以第1层为例，interval就是20毫秒。
     * 由于下一层时间轮的滴答时长就是上一层的总时长，因此，第2层的滴答时长就是20毫秒，总时长是400毫秒，以此类推
     * **/
    private final Long interval;

    /**时间轮下的所有Bucket对象，也就是所有TimerTaskList对象**/
    private final TimerTaskList[] buckets;

    /**
     * 当前时间戳，将它设置成小于当前时间的最大滴答时长的整数倍。
     * 举个例子，假设滴答时长是20毫秒，当前时间戳是123毫秒，那么，currentTime会被调整为120毫秒
     */
    private Long currentTime;

    /**
     * 按需创建上层时间轮的。
     * 这也就是说，当有新的定时任务到达时，会尝试将其放入第1层时间轮。
     * 如果第1层的interval无法容纳定时任务的超时时间，就现场创建并配置好第2层时间轮，并再次尝试放入，如果依然无法容纳，那么，就再创建和配置第3层时间轮，以此类推，直到找到适合容纳该定时任务的第N层时间轮
     */
    private TimingWheel overflowWheel;
    
    /**
     * Instantiates a new Timing wheel.
     *
     * @param tickMs      the tick ms
     * @param wheelSize   the wheel size
     * @param startMs     the start ms 时间轮对象被创建时的起始时间戳
     * @param taskCounter the task counter
     * @param queue       the queue
     */
    TimingWheel(final Long tickMs, final Integer wheelSize, final Long startMs, final AtomicInteger taskCounter, final DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.interval = tickMs * wheelSize;
        this.currentTime = startMs - (startMs % tickMs);
        this.buckets = new TimerTaskList[wheelSize];
    }

    /**
     * 创建高层次时间轮，每层的轮子数都是相同的
     */
    private synchronized void addOverflowWheel() {
        if (overflowWheel == null) {
            overflowWheel = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue);
        }
    }
    
    /**
     * Add boolean.
     *
     * @param taskEntry the task entry
     * @return the boolean
     */
    boolean add(final TimerTaskList.TimerTaskEntry taskEntry) {
        // 获取定时任务的过期时间戳
        Long expirationMs = taskEntry.getExpirationMs();
        // 如果任务已经被取消，则无需添加
        if (taskEntry.cancelled()) {
            return false;
        }
        // 任务是否超时过期
        if (expirationMs < currentTime + tickMs) {
            return false;
        }
        // 任务超时时间在本轮时间轮覆盖时间范围内
        if (expirationMs < currentTime + interval) {
            //Put in its own bucket
            long virtualId = expirationMs / tickMs;
            int index = (int) (virtualId % wheelSize);
            TimerTaskList bucket = this.getBucket(index);
            bucket.add(taskEntry);
            // 设置过期时间，如果时间变更过，说明Bucket是新增的或者复用，则将其加入延迟队列
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket);
            }
            return true;
        }
        // 否则创建层级时间轮
        if (overflowWheel == null) {
            addOverflowWheel();
        }
        return overflowWheel.add(taskEntry);
    }
    
    /**
     * Advance clock.
     *
     * @param timeMs the time ms
     */
    void advanceClock(final long timeMs) {
        if (timeMs >= currentTime + tickMs) {
            currentTime = timeMs - (timeMs % tickMs);
        }
        if (overflowWheel != null) {
            overflowWheel.advanceClock(currentTime);
        }
    }
    
    private TimerTaskList getBucket(final int index) {
        TimerTaskList bucket = buckets[index];
        if (bucket == null) {
            synchronized (this) {
                bucket = buckets[index];
                if (bucket == null) {
                    bucket = new TimerTaskList(taskCounter);
                    buckets[index] = bucket;
                }
            }
        }
        return bucket;
    }
    
}
