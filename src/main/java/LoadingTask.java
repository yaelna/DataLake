import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import org.apache.openjpa.util.UnsupportedException;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.TaskGigaSpace;

import java.io.*;
import java.util.*;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class LoadingTask implements DistributedTask<Long, Long> {

    @TaskGigaSpace
    private transient GigaSpace gigaSpace;
    private final String typeName;
    private File csv;
    private List<String> fieldNames;
    private long partitionZeroInitialPos;
    private long fileNetSize;
    private int bytesBufferSize = 1024 * 4;
    private int myPartitionId;
    private int partitionsNum;

    LoadingTask(GigaSpace gigaSpace, File csv, String typeName) throws IOException {
        if (csv.length() == 0) throw new UnsupportedException("Invalid csv file was submitted, file is empty");
        this.gigaSpace = gigaSpace;
        this.csv = csv;
        this.typeName = typeName;
        this.fieldNames = extractFieldsNames();
    }

    public Long execute() throws Exception {
        SpaceImpl spaceImpl = gigaSpace.getSpace().getDirectProxy().getSpaceImplIfEmbedded();
        this.myPartitionId = spaceImpl.getPartitionIdOneBased() - 1;
        this.partitionsNum = spaceImpl.getClusterInfo().getNumberOfPartitions();
        SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(typeName).routingProperty("Partition").create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
        if (fileNetSize <= 0) return null;
        long chunk = fileNetSize / partitionsNum;
        long initialStartPos = partitionZeroInitialPos + chunk * myPartitionId;

        FileInputStream stream = new FileInputStream(csv);
        stream.getChannel().position(initialStartPos);
        BufferedInputStream inputStream = new BufferedInputStream(stream, bytesBufferSize);
        int counter = findStart(inputStream);
        int b = inputStream.read();
        counter++;
        long numOfObjects = 0;
        while (counter < chunk) {
            while (counter < chunk && b != (int) '\n' && b != (int) '\r') { // collect one line
                Map<String, Object> fields = new HashMap<String, Object>();
                for (String fieldName : fieldNames) {
                    List<Character> toString = new ArrayList<Character>();
                    while (counter < chunk && b != ',' && b != (int) '\n' && b != (int) '\r') {   // collect one field value
                        toString.add((char) b);
                        b = inputStream.read();
                        counter++;
                        if (counter == chunk && b != (int) '\n' && b != 0) {
                            chunk += 1024;
                        }
                    }
                    fields.put(fieldName, new String(toCharArray(toString)));
                    b = inputStream.read();
                    counter++;
                }
                fields.put("Partition", myPartitionId);
                SpaceDocument document = new SpaceDocument(typeName, fields);
                gigaSpace.write(new SpaceDocument(typeName, fields));
                numOfObjects++;
            }
            b = inputStream.read();
            counter++;
        }
        inputStream.close();
        return numOfObjects;
    }

    public Long reduce(List<AsyncResult<Long>> asyncResults) throws Exception {
        long res = 0;
        for (AsyncResult<Long> result : asyncResults) {
            if (result.getException() != null) {
                throw result.getException();
            }
            res += result.getResult();
        }
        return res;
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

    private int findStart(BufferedInputStream inputStream) throws IOException {
        int b = inputStream.read();
        int counter = 1;
        while (b != (int) '\n') {
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
