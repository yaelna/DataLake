/**
 * @author Yael Nahon
 * @since 12.1 .
 */

import org.apache.openjpa.util.UnsupportedException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class Tests {

    @Test
    public void onePartitionEmptyCsvTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/empty.csv")
                    , "onePartitionEmptyCsvTest", 1, 0);
            test.execute();
            Assert.fail();
        } catch (UnsupportedException e) {
            System.out.println("onePartitionEmptyCsvTest succeed");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void onePartitionOnlyHeaderCsvTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/onlyHeader.csv")
                    , "onePartitionOnlyHeaderCsvTest", 1, 0);
            Assert.assertEquals(new Integer(0), test.execute());
        } catch (UnsupportedException e) {
            System.out.println("onePartitionOnlyHeaderCsvTest succeed");
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void onePartitionEvenNumberOfEvenLinesTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "onePartitionEvenNumberOfEvenLinesTest", 1, 0);
            Assert.assertEquals(new Integer(4), test.execute());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void TwoPartitionEvenNumberOfEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "TwoPartitionEvenNumberOfEvenLinesTest", 2, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "TwoPartitionEvenNumberOfEvenLinesTest", 2, 1);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void ThreePartitionEvenNumberOfEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "ThreePartitionEvenNumberOfEvenLinesTest", 3, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "ThreePartitionEvenNumberOfEvenLinesTest", 3, 1);
            TestLoadingTask part3 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfEvenLines.csv")
                    , "ThreePartitionEvenNumberOfEvenLinesTest", 3, 2);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(1), part2.execute());
            Assert.assertEquals(new Integer(1), part3.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void onePartitionEvenNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "onePartitionEvenNumberOfUnEvenLinesTest", 1, 0);
            Assert.assertEquals(new Integer(4), test.execute());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void TwoPartitionEvenNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "TwoPartitionEvenNumberOfUnEvenLinesTest", 2, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "TwoPartitionEvenNumberOfUnEvenLinesTest", 2, 1);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void ThreePartitionEvenNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "ThreePartitionEvenNumberOfUnEvenLinesTest", 3, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "ThreePartitionEvenNumberOfUnEvenLinesTest", 3, 1);
            TestLoadingTask part3 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/evenNumberOfUnEvenLines.csv")
                    , "ThreePartitionEvenNumberOfUnEvenLinesTest", 3, 2);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(1), part2.execute());
            Assert.assertEquals(new Integer(1), part3.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void onePartitionOddNumberOfEvenLinesTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "onePartitionOddNumberOfEvenLinesTest", 1, 0);
            Assert.assertEquals(new Integer(5), test.execute());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void TwoPartitionOddNumberOfEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "TwoPartitionOddNumberOfEvenLinesTest", 2, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "TwoPartitionOddNumberOfEvenLinesTest", 2, 1);
            Assert.assertEquals(new Integer(3), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void ThreePartitionOddNumberOfEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "ThreePartitionOddNumberOfEvenLinesTest", 3, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "ThreePartitionOddNumberOfEvenLinesTest", 3, 1);
            TestLoadingTask part3 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfEvenLines.csv")
                    , "ThreePartitionOddNumberOfEvenLinesTest", 3, 2);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
            Assert.assertEquals(new Integer(1), part3.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void onePartitionOddNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask test = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "onePartitionOddNumberOfUnEvenLinesTest", 1, 0);
            Assert.assertEquals(new Integer(5), test.execute());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void TwoPartitionOddNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "TwoPartitionOddNumberOfUnEvenLinesTest", 2, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "TwoPartitionOddNumberOfUnEvenLinesTest", 2, 1);
            Assert.assertEquals(new Integer(3), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void ThreePartitionOddNumberOfUnEvenLinesTest() {
        try {
            TestLoadingTask part1 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "ThreePartitionOddNumberOfUnEvenLinesTest", 3, 0);
            TestLoadingTask part2 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "ThreePartitionOddNumberOfUnEvenLinesTest", 3, 1);
            TestLoadingTask part3 = new TestLoadingTask(new File("/home/yaeln-pcu/IdeaProjects/DataLakeTaskLoading/src/test/resources/oddNumberOfUnEvenLines.csv")
                    , "ThreePartitionOddNumberOfUnEvenLinesTest", 3, 2);
            Assert.assertEquals(new Integer(2), part1.execute());
            Assert.assertEquals(new Integer(2), part2.execute());
            Assert.assertEquals(new Integer(1), part3.execute());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


}
