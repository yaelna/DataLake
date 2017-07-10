import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import org.apache.openjpa.util.UnsupportedException;
import org.omg.IOP.FORWARDED_IDENTITY;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.TaskGigaSpace;

import java.io.*;
import java.util.*;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class Main {

    private static int myPartitionId = 2; //(should start from 0)
    private static int partitions = 3;
    private static int additionalBytesInterval = 100;
    private static File csv = new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/main/resources/fhv_tripdata_2015-01.csv");

    public static void main(String[] args) throws IOException {
        LoadingTask task = new LoadingTask(null, csv, "MyRow");
        try {
            task.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //TODO - maybe register type before task , on all partitions
    }

    public static class LoadingTask implements DistributedTask<Integer, Integer> {

        @TaskGigaSpace
        private transient GigaSpace gigaSpace;
        private final String typeName;
        private File csv;
        private List<String> fieldNames;
        private int partitionZeroInitialPos;
        private static long fileNetSize;

        public LoadingTask(GigaSpace gigaSpace, File csv, String typeName) throws IOException {
            if(csv.length() == 0) throw new UnsupportedException("Invalid csv file was submitted, file is empty");
            this.gigaSpace = gigaSpace;
            this.csv = csv;
            this.typeName = typeName;
            this.fieldNames = extractFieldsNames();
        }

        private List<String> extractFieldsNames() throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(csv));
            String header = reader.readLine();
            String[] strings = header.split(",");
            this.partitionZeroInitialPos = header.length();
            this.fileNetSize = csv.length() - partitionZeroInitialPos - 2;
            reader.close();
            return Arrays.asList(strings);
        }

        public Integer reduce(List<AsyncResult<Integer>> asyncResults) throws Exception {
            return null;
        }

        public Integer execute() throws Exception {
            if(fileNetSize <= 0) return null;
            int chunk = (int) fileNetSize / partitions;
            int initialStartPos = partitionZeroInitialPos + chunk*myPartitionId;
            byte[] bytes = getBytes(chunk, initialStartPos);
            int i = findStart(bytes);
            SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(typeName).routingProperty("Partition").create();
//            gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
            boolean stop = false;
            while (i> 0 && i < bytes.length) {
                while (!stop && i < bytes.length && bytes[i] != '\n' && bytes[i] != '\r') { // collect one line
                    Map<String, Object> fields = new HashMap<String, Object>();
                    for (String fieldName : fieldNames) {
                        List<Character> toString = new ArrayList<>();
                        while (i < bytes.length && bytes[i] != ',' && bytes[i] != '\n' && bytes[i] != '\r') {   // collect one field value
                            toString.add((char) bytes[i]);
                            i++;
                            if (i == bytes.length && !stop) {
                                bytes = getAdditionalBytes(initialStartPos + chunk);
                                i = 0;
                                stop = true;
                            }
                        }
                        fields.put(fieldName, new String(toCharArray(toString)));
                        i++;
                    }
                    fields.put("Partition", myPartitionId);
                    SpaceDocument document = new SpaceDocument(typeName, fields);
                    System.out.println(document.toString());
//                    gigaSpace.write(new SpaceDocument(typeName, fields));
                }
                i++;
            }

            return null;
        }

        private void printBytes(byte[] bytes) {
            for (byte aByte : bytes) {
                System.out.println("b = "+(char)aByte);
            }
        }

        private int findStart(byte[] bytes) {
            for (int i = 0; i < bytes.length; i++) {
                if(bytes[i] == '\n' || bytes[i] == 0){
                    return i;
                }
            }
            return -1;
        }

        private byte[] getAdditionalBytes(int initialStartPos) throws IOException {
            List<Byte> additionalBytes = new ArrayList<>();
            while (true){
                byte[] bytes = getBytes(additionalBytesInterval, initialStartPos);
                for (byte b : bytes){
                    if(b == 0 || b == '\n'){
                        return toByteArray(additionalBytes);
                    }
                    additionalBytes.add(b);
                }
            }
        }

        private byte[] toByteArray(List<Byte> additionalBytes) {
            byte[] res = new byte[additionalBytes.size()];
            for (int i = 0; i < additionalBytes.size(); i++) {
                res[i] = additionalBytes.get(i);
            }
            return res;
        }

        private byte[] getBytes(int chunkSize, int initialStartPos) throws IOException {
            byte[] bytes = new byte[chunkSize];
            FileInputStream inputStream = new FileInputStream(csv);
            inputStream.getChannel().position(initialStartPos);
            inputStream.read(bytes);
            inputStream.close();
            return bytes;
        }

        private char[] toCharArray(List<Character> toString) {
            char[] res = new char[toString.size()];
            for (int i = 0; i < toString.size(); i++) {
                res[i] = toString.get(i);
            }
            return res;
        }

        private int getFirstNewLineIndex(byte[] bytes) {
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == '\n') return i;
            }
            return -1;
        }

    }

}
