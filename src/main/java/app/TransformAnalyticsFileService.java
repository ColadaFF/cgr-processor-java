package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.control.Try;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class TransformAnalyticsFileService {
    private static final ObjectMapper mapper = new ObjectMapper();

    public Try<Void> transformFile(String path) {
        Objects.requireNonNull(path);

        var newPath = path.replaceAll("json", "parquet");
        return readFile(path)
                .mapTry(bytes -> mapper.readValue(bytes, Item[].class))
                .flatMap(items -> writeParquetFile(items, newPath));
    }


    private Try<Void> writeParquetFile(Item[] items, String localFileName) {
        return Try.withResources(() -> {
                    ensureDirectory(localFileName);
                    HadoopOutputFile outputFile = HadoopOutputFile.fromPath(
                            new Path(localFileName),
                            new Configuration()
                    );
                    return AvroParquetWriter
                            .<Item>builder(outputFile)
                            .withSchema(Item.getClassSchema())
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                            .build();
                })
                .of(writer -> {
                    for (Item item : items) {
                        writer.write(item);
                    }
                    return null;
                });
    }


    private void ensureDirectory(String filePath) {
        String directoryName = Arrays.stream(filePath.split("/"))
                .takeWhile(s -> !s.endsWith(".parquet"))
                .collect(Collectors.joining("/"));
        File directory = new File(directoryName);
        if (!directory.exists()) {
            directory.mkdirs();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
    }

    private Try<BufferedReader> readFile(String unsafeFilePath) {
        return Try.of(() -> new FileReader(unsafeFilePath))
                .map(BufferedReader::new);
    }
}
