/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package cn.xdf.acdc.connect.hdfs.hive;

import cn.xdf.acdc.connect.hdfs.initialize.StoreConfig;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetaStoreAccessTask implements Runnable {

    private final CyclicBarrier cyclicBarrier;

    private final HiveMetaStore hiveMetaStore;

    private final StoreConfig storeConfig;

    private final AtomicInteger runSuccessCounter;

    private final int runTimes;

    private final CountDownLatch cl;

    public MetaStoreAccessTask(
        final CyclicBarrier cyclicBarrier,
        final HiveMetaStore hiveMetaStore,
        final StoreConfig storeConfig,
        final int runTimes,
        final AtomicInteger runSuccessCounter,
        final CountDownLatch cl
    ) {

        this.cyclicBarrier = cyclicBarrier;
        this.hiveMetaStore = hiveMetaStore;
        this.storeConfig = storeConfig;
        this.runTimes = runTimes;
        this.runSuccessCounter = runSuccessCounter;
        this.cl = cl;
    }

    @Override
    public void run() {
        for (int i = 0; i < runTimes; i++) {
            try {
                cyclicBarrier.await();
                hiveMetaStore.getTable(storeConfig.database(), storeConfig.table());
                log.info("Get table success :" + Thread.currentThread().getId());
                runSuccessCounter.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Get table occur exception, thread be interrupted", e);
            } catch (BrokenBarrierException e) {
                Thread.currentThread().interrupt();
                log.warn("Get table occur exception, barrier be broken", e);
            }
        }

        cl.countDown();
    }
}
