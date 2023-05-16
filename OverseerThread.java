import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class OverseerThread implements Runnable{
    int id;
    Long start;
    Long end;
    File file;
    public AtomicInteger inQueue;

    ExecutorService overSeerPool;
    ExecutorService workerPool;
    Long P;
    String productsFilePath;

    public OverseerThread(int id, Long P, Long start, Long end, AtomicInteger inQueue, File file, ExecutorService overSeerPool, ExecutorService workerPool, String productsFilePath) {
        this.start = start;
        this.end = end;
        this.inQueue = inQueue;
        this.file = file;
        this.overSeerPool = overSeerPool;
        this.id = id;
        this.workerPool = workerPool;
        this.P = P;
        this.productsFilePath = productsFilePath;
    }

    @Override
    public void run() {
        Long pos = start;
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file.getPath(), "r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            raf.seek(start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        while(pos < end){
            try {
                String str = raf.readLine();
                if(Integer.parseInt(str.split(",")[1]) > 0){
                    Semaphore semaphore = new Semaphore(0);
                    AtomicInteger elemsNo = new AtomicInteger(Integer.parseInt(str.split(",")[1]));
                    File ordersFile = new File(productsFilePath);
                    List<Long> indexes = Shop.getIndexes(this.P, ordersFile);
                    for(int i = 0; i < indexes.size(); i++){
                        if(i < indexes.size() - 1){
                            workerPool.submit(new WorkerThread(i, indexes.get(i), indexes.get(i + 1), new File(productsFilePath), elemsNo, workerPool, new String(str.split(",")[0]), semaphore));
                        }
                        else{
                            workerPool.submit(new WorkerThread(i, indexes.get(i), ordersFile.length(), new File(productsFilePath), elemsNo, workerPool, new String(str.split(",")[0]), semaphore));
                        }
                    }
                    synchronized (this){
                        semaphore.acquire();
                        File ordersOutFile = new File("orders_out.txt");
                        if(!ordersOutFile.exists()){
                            ordersOutFile.createNewFile();
                        }
                        FileWriter ordersOutFW = new FileWriter(ordersOutFile.getAbsoluteFile(), true);
                        BufferedWriter ordersOutBW = new BufferedWriter(ordersOutFW);
                        ordersOutBW.write(str + ",shipped\n");
                        ordersOutBW.close();
                    }
                }
                pos = pos + str.length() + 1;
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        int left = inQueue.decrementAndGet();
        if(left == 0){
            overSeerPool.shutdown();
            workerPool.shutdown();
        }
    }
}
