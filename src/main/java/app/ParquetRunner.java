package app;

import com.google.gson.Gson;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.BaseStream;

public class ParquetRunner {
    private static final Logger logger = LogManager.getLogger(ParquetRunner.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        var dataPath = System.getenv("DATA_PATH");
        System.out.println("dataPath = " + dataPath);
        var service = new TransformAnalyticsFileService();

        Path internalPath = Paths.get(dataPath);
        fromPath(internalPath)
                .flatMap(s -> Mono.fromRunnable(() -> service.transformFile(s))
                        .subscribeOn(Schedulers.boundedElastic())
                )
                .subscribe(
                        logger::info,
                        logger::error,
                        () -> {
                            logger.info("Done");
                            System.exit(0);
                        }
                );

        Thread.sleep(Long.MAX_VALUE);
    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.walk(path, FileVisitOption.FOLLOW_LINKS),
                        Flux::fromStream,
                        BaseStream::close
                )
                .filter(path1 -> {
                    String fileName = path1.toString();
                    return FilenameUtils.getExtension(fileName).equals("json");
                })
                .doOnNext(logger::info)
                .map(Path::toString)
                .doOnNext(logger::info);
    }
}
