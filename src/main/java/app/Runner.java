package app;

import com.google.gson.*;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class Runner {
    public static void main(String[] args) throws InterruptedException {
        final Gson gson = provideGson();
        var path = System.getenv("PROGRAM_PATH");
        var limit = Optional.ofNullable(System.getenv("LIMIT"))
                .map(Integer::parseInt)
                .orElse(50);

        Flux.range(0, 5300)
                .flatMap(page -> {
                    Configuration configuration = new Configuration(
                            path,
                            limit,
                            limit * page
                    );
                    return Mono.fromCompletionStage(callProgram(configuration))
                            .subscribeOn(Schedulers.boundedElastic())
                            .map(inputStream -> Try
                                    .of(() -> new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                                    .flatMapTry(reader -> Try
                                            .withResources(() -> new BufferedReader(reader))
                                            .of(bufferedReader -> bufferedReader.lines()
                                                    .map(line -> gson.fromJson(line, Response.class))
                                                    .filter(Objects::nonNull)
                                                    .findFirst()
                                            )
                                    )
                                    .getOrElse(Optional.empty())
                            )
                            .flatMap(response -> response.map(Mono::just)
                                    .orElse(Mono.empty()));
                }, 20)
                .subscribe(
                        System.out::println,
                        System.err::println,
                        () -> System.exit(0)
                );

        Thread.sleep(Long.MAX_VALUE);
    }


    private static CompletableFuture<InputStream> callProgram(Configuration configuration) {
        Objects.requireNonNull(configuration, "Path can not be null.");
        try {
            ProcessBuilder pb = new ProcessBuilder("npm", "run", "local:test");
            pb.directory(new File(configuration.path));
            Map<String, String> environment = pb.environment();
            environment.put("QUERY_LIMIT", Objects.toString(configuration.limit));
            environment.put("QUERY_SKIP", Objects.toString(configuration.skip));
            Process process = pb.inheritIO().start();
            ProcessHandle processHandle = process.toHandle();
            return processHandle.onExit()
                    .thenApply(ignored -> process.getInputStream());
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }


    private static Gson provideGson() {
        return new GsonBuilder()
                .registerTypeAdapter(Response.class, new ResponseAdapter())
                .create();
    }

    static class ResponseAdapter implements JsonDeserializer<Response> {

        @Override
        public Response deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject root = json.getAsJsonObject();
            String type = root.get("type").getAsString();
            ResponseType responseType = ResponseType.valueOf(type);
            return switch (responseType) {
                case FAILURE -> context.deserialize(root, FailureResponse.class);
                case SUCCESS -> context.deserialize(root, SuccessResponse.class);
            };
        }
    }


    record Configuration(
            String path,
            int limit,
            int skip
    ) {
    }

    enum ResponseType {
        FAILURE,
        SUCCESS
    }

    interface Response {
        ResponseType type();
    }

    record SuccessResponse(
            Integer failureCount,
            Integer successCount,
            List<String> failures,
            List<String> successes
    ) implements Response {

        @Override
        public ResponseType type() {
            return ResponseType.SUCCESS;
        }
    }

    record FailureResponse(
            String errorMessage
    ) implements Response {

        @Override
        public ResponseType type() {
            return ResponseType.FAILURE;
        }
    }
}
