import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerThread implements Runnable {
    int id;
    Long start;
    Long end;

    String order;

    AtomicInteger overseerInQueue;

    public WorkerThread(int id, Long start, Long end, File file, AtomicInteger inQueue, ExecutorService workerPool, String order, Semaphore semaphore) {
        this.id = id;
        this.start = start;
        this.end = end;
        this.file = file;
        this.inQueue = inQueue;
        this.workerPool = workerPool;
        this.order = order;
        this.semaphore = semaphore;
    }

    File file;
    public AtomicInteger inQueue;

    ExecutorService workerPool;
    Long P;
    Semaphore semaphore;

    @Override
    public void run() {
        Long pos = start;
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            bufferedReader.skip(start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while(pos < end){
            try {
                String str = bufferedReader.readLine();
                if(str.split(",")[0].equals(order)) {
                    synchronized (this){
                        File ordersOutFile = new File("order_products_out.txt");
                        if(!ordersOutFile.exists()){
                            ordersOutFile.createNewFile();
                        }
                        FileWriter ordersOutFW = new FileWriter(ordersOutFile.getAbsoluteFile(), true);
                        BufferedWriter ordersOutBW = new BufferedWriter(ordersOutFW);
                        ordersOutBW.write(str + ",shipped\n");
                        ordersOutBW.close();
                    }
                    inQueue.decrementAndGet();
                }
                pos = pos + str.length() + 1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        int left = inQueue.get();
        if(left == 0 && semaphore.availablePermits() == 0){
            semaphore.release();
        }
    }
}
