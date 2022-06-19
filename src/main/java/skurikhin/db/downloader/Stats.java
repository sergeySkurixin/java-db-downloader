package skurikhin.db.downloader;

import java.util.concurrent.atomic.AtomicLong;

public class Stats {
   public final long startTime;
   public long finishTime;
   public final AtomicLong extracted = new AtomicLong();
   public final AtomicLong rounds = new AtomicLong();

    public Stats() {
        this.startTime = System.currentTimeMillis();
    }

    Stats finish() {
        finishTime = System.currentTimeMillis();
        return this;
    }

    long durationMs() {
        long t = this.finishTime != 0 ? finishTime : System.currentTimeMillis();
        return t - startTime;
    }
}