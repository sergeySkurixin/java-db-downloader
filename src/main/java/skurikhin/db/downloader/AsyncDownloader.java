package skurikhin.db.downloader;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import skurikhin.db.config.DbConfig;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static skurikhin.db.downloader.Utils.nextBatchIds;

public class AsyncDownloader {

    public static final int BATCH_SIZE = 100;
    public static final int MAX_ROUNDS = 20000;

    @Language("SQL")
    final String SQL = "SELECT phone, p.bal_id " +
            "FROM c24.nsi_person p " +
            "WHERE     phone IN ";

    void asyncDownload(DbConfig dbConfig) throws SQLException, IOException, InterruptedException {
        String baseFolder = "/Users/skurikhin/work/2022_06_17_clients-without-bal-id/";

        HikariDataSource dataSource = buildDataSource(dbConfig);
        try (LineIterator lineIterator = FileUtils.lineIterator(new File(baseFolder + "output.csv"));
             BufferedWriter writer = new BufferedWriter(new FileWriter(baseFolder + "result.txt"))) {
            makeAsyncLoop(lineIterator, writer, dataSource);
        }
    }

    private HikariDataSource buildDataSource(DbConfig dbConfig) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbConfig.getUrl());
        config.setUsername(dbConfig.getUser());
        config.setPassword(dbConfig.getPassword());
        return new HikariDataSource(config);
    }

    private Stats makeAsyncLoop(LineIterator lineIterator, BufferedWriter writer, DataSource dataSource) throws SQLException, IOException, InterruptedException {
        Stats stats = new Stats();
        ArrayBlockingQueue<Long> input = new ArrayBlockingQueue<>(5000);
        BlockingQueue<String> output = new ArrayBlockingQueue<>(2000);
        ArrayList<Worker> workers = new ArrayList<>();
        int workerThreads = 30;

        for (int i = 0; i < workerThreads; i++) {
            workers.add(new Worker(stats, input, output));
        }
        ExecutorService workerExecutor = Executors.newFixedThreadPool(workerThreads);
        Thread reporter = new ReporterThread(stats, BATCH_SIZE, input, output);
        reporter.start();
        ExecutorService outputExec = Executors.newSingleThreadExecutor();
        outputExec.execute(() -> {
            outputAction(writer, output);
        });
        for (Worker worker : workers) {
            workerExecutor.execute(() -> {
                try {
                    worker.start(dataSource);
                } catch (Throwable e) {
                    System.err.println(e);
                }
            });
        }

        while (stats.rounds.get() < MAX_ROUNDS) {
            stats.rounds.incrementAndGet();
            List<Long> ids = nextBatchIds(lineIterator, BATCH_SIZE);
            if (ids.isEmpty()) {
                System.out.println("That is all!!!!");
                break;
            }
            for (Long id : ids) {
                input.put(id);
            }
        }
        workers.forEach(w -> w.needToFinish = true);
        System.out.println(String.format("Finished, rounds=%d, durationMs=%s", stats.rounds.get(), stats.durationMs()));
        reporter.interrupt();
        reporter.join();
        System.out.println("Reporter joined");

        workerExecutor.shutdown();
        workerExecutor.awaitTermination(10, TimeUnit.SECONDS);

        outputExec.shutdown();
        outputExec.awaitTermination(10, TimeUnit.SECONDS);

        return stats.finish();
    }

    private void outputAction(BufferedWriter writer, BlockingQueue<String> output) {
        long lastFlush = System.currentTimeMillis();
        while (true) {
            try {
                String rec = output.poll(50, TimeUnit.SECONDS);
                if (rec != null) {
                    writer.append(rec);
                    writer.newLine();
                } else {
                    System.out.println("Finish writer!");
                    return;
                }
                long t = System.currentTimeMillis();
                if (t - lastFlush > 1_000) {
                    writer.flush();
                    lastFlush = t;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @NotNull
    private String generateSql(List<Long> ids) {
        String args = ids.size() == 1 ? "" : StringUtils.repeat("?,", ids.size() - 1);
        return SQL + "(" + args + "?)";
    }

    private void fillPsArgs(List<Long> ids, PreparedStatement ps) throws SQLException {
        for (int i = 0; i < ids.size(); i++) {
            ps.setLong(i + 1, ids.get(i));
        }
    }

    class Worker {
        final BlockingQueue<Long> inputQueue;
        final BlockingQueue<String> resQueue;
        final Stats stats;
        volatile boolean needToFinish;

        public Worker(Stats stats, BlockingQueue<Long> input, BlockingQueue<String> resQueue) {
            this.stats = stats;
            this.inputQueue = input;
            this.resQueue = resQueue;
        }

        void start(DataSource dataSource) throws SQLException, InterruptedException {
            try (Connection con = dataSource.getConnection()) {
                List<Long> buffer = new ArrayList<>();
                while (true) {
                    int drain = inputQueue.drainTo(buffer, BATCH_SIZE);
                    if (drain == 0 && needToFinish) {
                        break;
                    }

                    PreparedStatement ps = con.prepareStatement(generateSql(buffer));
                    fillPsArgs(buffer, ps);
                    ResultSet rs = ps.executeQuery();

                    while (rs.next()) {
                        stats.extracted.incrementAndGet();
                        resQueue.put(String.format("%s;%s", rs.getString(1), rs.getString(2)));
                    }
                    buffer.clear();
                }
            }
            System.out.println("Finished worker");
        }
    }
}
