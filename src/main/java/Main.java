import com.gigaspaces.async.AsyncFuture;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.core.GigaSpace;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class Main {
    private static File csv = new File("/home/yaeln-pcu/Downloads/yellow_tripdata_2016-01.csv");

    public static void main(String[] args) throws IOException {
        Admin admin = new AdminFactory().addGroup("dataLake").createAdmin();
        GigaSpace gigaSpace = admin.getSpaces().waitFor("fastDataLake").getGigaSpace();
        try {
            LoadingTask task = new LoadingTask(gigaSpace, csv, "MyRow");
            System.out.println("submitting task");
            long start = System.currentTimeMillis();
            AsyncFuture<Long> res = gigaSpace.execute(task);
            while (!res.isDone()) {
                Thread.sleep(1000);
            }
            long duration = TimeUnit.SECONDS.toSeconds(System.currentTimeMillis() - start);
            System.out.println("finished loading " + res.get() + " objects, load took " + duration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}



