import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class SensorDataProduzent {

    public static void produce(int sendMessageCount, long intervall){

        Sensor sensor1 = new Sensor("S1");

        long time = System.currentTimeMillis();

        Producer<Long, SensorDaten> producer = createProducer();

        try{
        for (long index = time; index < time + sendMessageCount; index++) {
                ProducerRecord<Long, SensorDaten> record = new ProducerRecord<>(ServerConfiguration.TOPIC, index, sensor1.produce());
                producer.send(record);
                Thread.sleep(intervall*1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            producer.flush();
            producer.close();
        }

    }

    private static Producer<Long, SensorDaten> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ServerConfiguration.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SensorDatenProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SensorDatenSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
