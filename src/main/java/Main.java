import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
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

    private static int myPartitionId = 0; //(should start from 0)
    private static int partitions = 2;
    private static File csv = new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/main/resources/fhv_tripdata_2015-01.csv");


    public static void main(String[] args) throws IOException {
        LoadingTask task = new LoadingTask(null, csv, "MyRow");
        try {
            task.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //TODO - maby register type before task , on all partitions

    }

    public static class LoadingTask implements DistributedTask<Integer, Integer> {

        private final String typeName;
        @TaskGigaSpace
        private transient GigaSpace gigaSpace;
        private File csv;
        private List<String> fieldNames;
        private int partitionZeroInitialPos;

        public LoadingTask(GigaSpace gigaSpace, File csv, String typeName) throws IOException {
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
            reader.close();
            return Arrays.asList(strings);
        }

        public Integer reduce(List<AsyncResult<Integer>> asyncResults) throws Exception {
            return null;
        }

        public Integer execute() throws Exception {
            FileReader in = new FileReader(csv);
            BufferedReader stream = new BufferedReader(in);
            int chunk = (int) (csv.length() / partitions);
            int initialStartPos = myPartitionId ==0 ? partitionZeroInitialPos : (int) ((csv.length() / partitions) * myPartitionId);
            char[] chars = new char[chunk];
            stream.read(chars, initialStartPos,  chunk + initialStartPos);
            stream.close();
            int i = myPartitionId == 0 ? partitionZeroInitialPos : getFirstNewLineIndex(chars);

            SpaceTypeDescriptor typeDescriptor = new SpaceTypeDescriptorBuilder(typeName).routingProperty("Partition").create();
//            gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);

            while (i < chars.length) {
                while (chars[i] != '\n') { // collect one line
                    Map<String, Object> fields = new HashMap<String, Object>();
                    for (String fieldName : fieldNames) {
                        List<Character> toString = new ArrayList<>();
                        while (chars[i] != ',' && chars[i] != '\n') {   // collect one field value
                            toString.add(chars[i]);
                            i++;
                        }
                        fields.put(fieldName, new String(toCharArray(toString)));
                        i++;
                    }
                    fields.put("Partition", myPartitionId);
                    String document = new SpaceDocument(typeName, fields).toString();
                    System.out.println(document);
//                    gigaSpace.write(new SpaceDocument(typeName, fields));
                }
            }

            return null;
        }

        private char[] toCharArray(List<Character> toString) {
            char[] res = new char[toString.size()];
            for (int i = 0; i < toString.size(); i++) {
                res[i] = toString.get(i);
            }
            return res;
        }

        private int getFirstNewLineIndex(char[] chars) {
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == '\n') return i;
            }
            return -1;
        }

    }

}
