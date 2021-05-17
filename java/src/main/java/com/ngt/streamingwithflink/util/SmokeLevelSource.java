package com.ngt.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-05-17 21:01
 */
public class SmokeLevelSource extends RichParallelSourceFunction<SmokeLevel> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {
        Random rand = new Random();
        while (running) {
            if (rand.nextGaussian() > 0.8) {
                ctx.collect(SmokeLevel.Low);
            }

            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
