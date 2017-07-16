import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.document.SpaceDocument;
import org.apache.openjpa.util.UnsupportedException;

import java.io.*;
import java.util.*;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class TestLoadingTask{

    private final String typeName;
    private File csv;
    private List<String> fieldNames;
    private long partitionZeroInitialPos;
    private long fileNetSize;
    private int bytesBufferSize = 1024 * 4;
    private int myPartitionId;
    private int partitionsNum;
    private int result =0;

    TestLoadingTask(File csv, String typeName, int partitions, int myPartitionId) throws IOException {
        if (csv.length() == 0) throw new UnsupportedException("Invalid csv file was submitted, file is empty");
        this.csv = csv;
        this.typeName = typeName;
        this.fieldNames = extractFieldsNames();
        this.myPartitionId = myPartitionId;
        this.partitionsNum = partitions;
    }

    private List<String> extractFieldsNames() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(csv));
        String header = reader.readLine();
        String[] strings = header.split(",");
        this.partitionZeroInitialPos = header.length();
        fileNetSize = csv.length() - partitionZeroInitialPos - 2;
        reader.close();
        return Arrays.asList(strings);
    }

    public Integer reduce(List<AsyncResult<Integer>> asyncResults) throws Exception {
        return null;
    }

    public Integer execute() throws Exception {
        if (fileNetSize <= 0) return null;
        int chunk = (int) fileNetSize / partitionsNum;
        long initialStartPos = partitionZeroInitialPos + chunk * myPartitionId;
        FileInputStream stream = new FileInputStream(csv);
        stream.getChannel().position(initialStartPos);
        BufferedInputStream inputStream = new BufferedInputStream(stream, bytesBufferSize);
        int counter = findStart(inputStream);
        int b = inputStream.read();
        counter++;
        boolean stop =false;
        while (counter < chunk && !stop) {
            while (counter < chunk && b != (int)'\n' && b != (int)'\r' && b != -1) { // collect one line
                Map<String, Object> fields = new HashMap<String, Object>();
                for (String fieldName : fieldNames) {
                    List<Character> toString = new ArrayList<Character>();
                    while (counter < chunk && b != (int)',' && b != (int)'\n' && b != (int)'\r' && b != -1) {   // collect one field value
                        toString.add((char) b);
                        b = inputStream.read();
                        counter++;
                        if (counter == chunk && b != (int)'\n' && b != -1) {
                            stop = true;
                            chunk += 1024;
                        }
                    }
                    fields.put(fieldName, new String(toCharArray(toString)));
                    b = inputStream.read();
                    counter++;
                    if (counter == chunk && b != (int)'\n' && b != -1) {
                        stop = true;
                        chunk += 1024;
                    }
                }
                fields.put("Partition", myPartitionId);
                SpaceDocument document = new SpaceDocument(typeName, fields);

                System.out.println(document.toString());
                result++;
            }
            b = inputStream.read();
            counter++;
            if (counter == chunk && b != (int) '\n' && b != -1) {
                chunk += 1024;
            }
        }
        inputStream.close();
        return result;
    }

    private int findStart(BufferedInputStream inputStream) throws IOException {
        int b = inputStream.read();
        int counter = 1;
        while (b != (int)'\n') {
            b = inputStream.read();
            counter++;
        }
        return counter;
    }

    private static char[] toCharArray(List<Character> toString) {
        char[] res = new char[toString.size()];
        for (int i = 0; i < toString.size(); i++) {
            res[i] = toString.get(i);
        }
        return res;
    }

}
