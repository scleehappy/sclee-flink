package me.sclee.flink.connectors.files;

import java.io.File;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;

public class FileConnectorMain {


    private static final String bucket = "files/sample_file.txt";

    public static void main(String[] args) throws Exception {
        FileConnectorMain main = new FileConnectorMain();
        File file = getFile(main);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final FileSource<String> source = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(), new Path(file.getAbsolutePath()))
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        DataStream<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                "file-source");

        sourceStream.print();
        env.execute("Flink App");
    }

    private static File getFile(FileConnectorMain main) {
        ClassLoader classLoader = main.getClass().getClassLoader();
        return new File(classLoader.getResource(bucket).getFile());
    }
}
