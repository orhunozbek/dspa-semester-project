package kafka;

import model.Event;
import model.EventFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import preparation.ReaderUtils;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.util.Properties;

public class EventKafkaProducer {

    public static void streamToKafka(ReaderUtils.Topic topic){
        Reader reader = null;

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                EventSerializer.class);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);


        KafkaProducer<String, Event> producer = new KafkaProducer<>(kafkaProps);

        try {

            File eventStreamFile = ReaderUtils.getFile(ReaderUtils.Directory.WorkingDirectory, topic);
            assert eventStreamFile != null;
            reader = Files.newBufferedReader(eventStreamFile.toPath());

            CSVFormat inputFormat = CSVFormat.newFormat('|')
                    .withHeader(ReaderUtils.getHeaderFor(ReaderUtils.Directory.WorkingDirectory, topic))
                    .withRecordSeparator('\n');

            CSVParser csvParser = new CSVParser(reader, inputFormat);

            for(CSVRecord record : csvParser) {
                Event obj = EventFactory.getEventFromTopicAndRecord(topic, record);
                producer.send(new ProducerRecord<>(obj.getTopicName(), obj));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
