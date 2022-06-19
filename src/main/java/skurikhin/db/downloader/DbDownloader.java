package skurikhin.db.downloader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import skurikhin.db.config.DbConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static skurikhin.db.downloader.Utils.nextBatchIds;

public class DbDownloader {

    public static final int BATCH_SIZE = 100;
    public static final int MAX_ROUNDS = 20000;

    @Language("SQL")
    final String SQL = "SELECT phone, p.bal_id " +
            "FROM c24.nsi_person p " +
            "WHERE     phone IN ";

    void syncDownload(DbConfig dbConfig) throws SQLException, IOException, InterruptedException {
        String baseFolder = "/Users/skurikhin/work/2022_06_17_clients-without-bal-id/";

        try (LineIterator lineIterator = FileUtils.lineIterator(new File(baseFolder + "output.csv"));
             BufferedWriter writer = new BufferedWriter(new FileWriter(baseFolder + "result.txt"));
             Connection con = DriverManager.getConnection(
                     dbConfig.getUrl(),
                     dbConfig.getUser(),
                     dbConfig.getPassword());) {
            makeLoop(lineIterator, writer, con);
        }
    }

    private Stats makeLoop(LineIterator lineIterator, BufferedWriter writer, Connection con) throws SQLException, IOException, InterruptedException {
        Stats stats = new Stats();
        Thread reporter = new ReporterThread(stats, BATCH_SIZE, Collections.emptyList(), Collections.emptyList());
        reporter.start();
        long readFromFileDuration = 0;
        long prepareStatementDuration = 0;
        long queryDuration = 0;
        long rsDuration = 0;
        long writeToFileDuration = 0;
        while (stats.rounds.get() < MAX_ROUNDS) {
            stats.rounds.incrementAndGet();
            long t0 = System.currentTimeMillis();
            List<Long> ids = nextBatchIds(lineIterator, BATCH_SIZE);
            readFromFileDuration += System.currentTimeMillis() - t0;
            if (ids.isEmpty()) {
                System.out.println("That is all!!!!");
                break;
            }
            t0 = System.currentTimeMillis();
            PreparedStatement ps = con.prepareStatement(generateSql(ids));
            fillPsArgs(ids, ps);
            prepareStatementDuration += System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            ResultSet rs = ps.executeQuery();
            queryDuration += System.currentTimeMillis() - t0;

            t0 = System.currentTimeMillis();
            while (rs.next()) {
                rsDuration += System.currentTimeMillis() - t0;
                stats.extracted.incrementAndGet();
                t0 = System.currentTimeMillis();
                writeToFile(writer, rs);
                writeToFileDuration += System.currentTimeMillis() - t0;
            }
        }
        System.out.printf("Finished, rounds=%d, durationMs=%s%n", stats.rounds.get(), stats.durationMs());
        reporter.interrupt();
        reporter.join();
        System.out.println("Reporter joined");

        System.out.printf("readFromFileDuration=%s, perc=%s%n", readFromFileDuration, readFromFileDuration * 100. / stats.durationMs());
        System.out.printf("prepareStatementDuration=%s, perc=%s%n", prepareStatementDuration, prepareStatementDuration * 100. / stats.durationMs());
        System.out.printf("queryDuration=%s, perc=%s%n", queryDuration, queryDuration * 100. / stats.durationMs());
        System.out.printf("rsDuration=%s, perc=%s%n", rsDuration, rsDuration * 100. / stats.durationMs());
        System.out.printf("writeToFileDuration=%s, perc=%s%n", writeToFileDuration, writeToFileDuration * 100. / stats.durationMs());

        return stats.finish();
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

    private void writeToFile(BufferedWriter writer, ResultSet rs) throws IOException, SQLException {
        writer.append(rs.getString(1));
        writer.append(';');
        writer.append(rs.getString(2));
        writer.newLine();
    }

}
