import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Shop {
    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        AtomicInteger inQueue = new AtomicInteger(0);
        File inFile = new File(args[0] + "/orders.txt");
        Long P = Long.parseLong(args[1]);
        List<Long> indexes = Shop.getIndexes(P, inFile);
        ExecutorService overSeerPool = Executors.newFixedThreadPool(P.intValue());
        ExecutorService workerPool = Executors.newFixedThreadPool(P.intValue());
        File orders_out = new File("orders_out.txt");
        File order_products_out = new File("order_products_out.txt");
        if(orders_out.exists()){
            orders_out.delete();
        }
        if(order_products_out.exists()){
            order_products_out.delete();
        }
        for(int i = 0; i < indexes.size(); i++) {
            if (i < indexes.size() - 1) {
                inQueue.incrementAndGet();
                overSeerPool.submit(new OverseerThread(i, P, indexes.get(i), indexes.get(i + 1), inQueue, new File(inFile.getPath()), overSeerPool, workerPool, args[0] + "/order_products.txt"));
            } else {
                inQueue.incrementAndGet();
                overSeerPool.submit(new OverseerThread(i, P, indexes.get(i), inFile.length(), inQueue, new File(inFile.getPath()), overSeerPool, workerPool, args[0] + "/order_products.txt"));
            }
        }
        long endTime = System.nanoTime();
        System.out.println(endTime - startTime);
    }

    public static List<Long> getIndexes(Long P, File inFile) throws IOException {
        Long fileSize = inFile.length();
        List<Long> indexes = new ArrayList<>();
        Long offset = 0L;
        while(offset < fileSize){
            indexes.add(offset);
            offset += fileSize / P;
        }
        for(int i = 1; i < indexes.size(); i++){
            Long off = indexes.get(i);
            RandomAccessFile fr = new RandomAccessFile(inFile, "r");
            fr.seek(off);
            while(fr.read() != '\n'){
                off++;
            }
            indexes.set(i, off + 1);
        }
        return indexes.stream().distinct().sorted().filter(x -> x != fileSize).toList();
    }
}
