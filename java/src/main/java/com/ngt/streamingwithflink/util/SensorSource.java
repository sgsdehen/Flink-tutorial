package com.ngt.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author ngt
 * @create 2021-05-17 20:53
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                ctx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }
            TimeUnit.MICROSECONDS.sleep(100);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
