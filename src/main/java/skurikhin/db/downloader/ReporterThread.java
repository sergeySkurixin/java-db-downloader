package skurikhin.db.downloader;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class ReporterThread extends Thread {
    private static final AtomicLong COUNTER = new AtomicLong();
    private final Stats stats;
    private final int workerBatchSize;
    private final Collection<Long> input;
    private final Collection<String> output;

    public ReporterThread(Stats stats, int workerBatchSize, Collection<Long> input, Collection<String> output) {
        this.stats = stats;
        this.workerBatchSize = workerBatchSize;
        this.input = input;
        this.output = output;
        setName("reporter_" + COUNTER.getAndIncrement());
    }

    @Override
    public synchronized void start() {
        reporterLoop();
    }

    private synchronized void reporterLoop() {
        System.out.println("Reporter started!");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                wait(1_000L);
            } catch (InterruptedException e) {
                break;
            }
            long rounds = stats.rounds.get();
            double clientsTps = rounds * workerBatchSize * 1000. / stats.durationMs();

            System.out.printf("rounds=%d, extracted=%d, checkedPhones=%s, inputSize=%s, outputSize=%s, clientsTps=%.2f%n",
                    rounds, stats.extracted.get(), rounds * workerBatchSize, input.size(), output.size(), clientsTps);
        }
        System.out.println("Reporter finished!");
    }
}
