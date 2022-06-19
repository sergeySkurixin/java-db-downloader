package skurikhin.db.downloader;

import org.apache.commons.io.LineIterator;

import java.util.ArrayList;
import java.util.List;

public final class Utils {
    private Utils() {
    }

    public static List<Long> nextBatchIds(LineIterator iterator, int batchSize) {
        int i = 0;
        List<Long> res = new ArrayList<>(batchSize);
        while (iterator.hasNext() && i++ < batchSize) {
            res.add(Long.parseLong(iterator.nextLine()));
        }
        return res;
    }
}
