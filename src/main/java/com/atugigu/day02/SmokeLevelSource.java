package com.atugigu.day02;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    public boolean running = true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        Random rand = new Random();

        while (running){
            if (rand.nextGaussian() > 0.8){
                ctx.collect(SmokeLevel.HIGH);
            } else {
                ctx.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
