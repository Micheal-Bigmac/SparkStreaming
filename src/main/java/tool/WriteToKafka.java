package tool;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by PC on 2017/1/16.
 */
public class WriteToKafka {
    private static final Logger LOG = LoggerFactory.getLogger(WriteToKafka.class);
    private List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();
    private KeyedMessage<String, byte[]> message = null;
    Producer<String, byte[]> producer = null;
    private static final String topic = "sparkStreamingTopic";

    public WriteToKafka() {
        producer = createProducer();
    }

    private Producer<String, byte[]> createProducer() {
        Properties properties = new Properties();
//        properties.put("zookeeper.connect", "name1.cvnavi.com:2181,name2.cvnavi.com:2181,name3.cvnavi.com:2181");// 声明zk
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 该属性表示你需要在消息被接收到的时候发送ack给发送者。以保证数据不丢失
        properties.put("request.required.acks", "1");
        properties.put("metadata.broker.list", "localhost:9092");
//        properties.put("metadata.broker.list", "data1.cvnavi.com:9092,data2.cvnavi.com:9092,data3.cvnavi.com:9092,data4.cvnavi.com:9092,data5.cvnavi.com:9092,data6.cvnavi.com:9092");// 声明kafka

        return new Producer<String, byte[]>(new ProducerConfig(properties));
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        WriteToKafka kafka = new WriteToKafka();
        String[] str={};
        File dir=new File("C:\\Users\\PC\\Downloads\\FlumeDataToKafka\\data1\\");
        if(dir.isDirectory()){
            File[] list = dir.listFiles();
            for(File tmp : list){

                String absolutePath = tmp.getAbsolutePath();
                System.out.println(absolutePath);
                kafka.writeData(absolutePath);
                Thread.currentThread().sleep(10000);
//                break;
            }

        }
    }

    public void writeData(String FilePath) throws IOException {

        List<String> strings = FileUtils.readLines(new File(FilePath));
        byte[] bytes;
        for (String tmp : strings) {
            bytes =  Bytes.fromHex(tmp);
            if (bytes.length > 8) {
                byte[] vehicleId = new byte[8];
                System.arraycopy(bytes, 4, vehicleId, 0, 8);


                byte[] dataType = new byte[4];
                System.arraycopy(bytes, 0, dataType, 0, 4);
                int dataTypeId = Bytes.toInt(dataType);

//                LOG.warn(dataTypeId + " 类型ID ");
                // 车辆ID
//                LOG.warn(Bytes.toLong(vehicleId) + "车辆ID");
                // 消息内容
                byte[] dataContent = new byte[bytes.length - 12];
                // 数组之间的复制
                System.arraycopy(bytes, 12, dataContent, 0, bytes.length - 12);

                message = new KeyedMessage<String, byte[]>(topic, String.valueOf(Bytes.toLong(vehicleId)), bytes);
                messages.add(message);
                if (messages.size() > 1000) {
                    producer.send(messages);
                    messages.clear();
                }
            }
        }
    }

    public static void test(String args[]) {
        JavaSparkContext sparkCntext = getSparkContext();
        JavaRDD<String> stringJavaRDD = sparkCntext.textFile("");
        JavaRDD<byte[]> binaryData = stringJavaRDD.map(new Function<String, byte[]>() {

            @Override
            public byte[] call(String v1) throws Exception {
                return Bytes.toBytesBinary(v1);
            }
        });


    }

    private static JavaSparkContext getSparkContext() {
        SparkConf sparkConf = new SparkConf().setAppName("writeToKafka").setMaster("local[2]");
        sparkConf.set("spark.serializer", org.apache.spark.serializer.KryoSerializer.class.getName());
        sparkConf.set("spark.kryoserializer.buffer.max", "512");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;
    }
}
