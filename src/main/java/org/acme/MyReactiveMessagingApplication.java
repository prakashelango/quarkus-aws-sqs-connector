package org.acme;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.quarkus.runtime.Startup;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.SqsOutboundMetadata;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import lombok.RequiredArgsConstructor;

import io.quarkus.logging.Log;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
@RequiredArgsConstructor
public class MyReactiveMessagingApplication {

    List<Domain> received = new CopyOnWriteArrayList<>();


    private final EventBus usageQueueEventBus;

    @Channel("out")
    Emitter<String> sqsEmitter;


    @ConsumeEvent("event_bus_channel")
    public void generateStreamOfStrings(Domain domain) {
        Log.infof("Consuming event bus message: %s", domain);

        var json = JsonObject.mapFrom(domain);

        String jsonString = json.toString();
        Log.infof("Json Emitter: %s", jsonString);
        sqsEmitter.send(jsonString);
    }



   /*@Outgoing("out")
    public Multi<Message<Domain>> produce() {
        Log.infof("Producer sending messages");
        return Multi.createFrom().ticks().every(Duration.of(300, ChronoUnit.MILLIS))
            .emitOn(Thread::startVirtualThread)
            .map(i -> {
                String message = String.valueOf(i);
                Log.infof("Producing message: %s", message);
                return Message.of(new Domain("name"+message), Metadata.of(SqsOutboundMetadata.builder().groupId(message).deduplicationId(message).build()))
                    .withNack(t -> {
                        System.out.println("error ");
                        t.printStackTrace();
                        return Uni.createFrom().voidItem().subscribeAsCompletionStage();
                    });
            });
    }*/




   @Startup
    public void playWithEvents() {
        Multi.createFrom().ticks().every(Duration.of(1, ChronoUnit.SECONDS))
            .subscribe().with(item -> usageQueueEventBus.send("event_bus_channel", new Domain("name"+item)));
    }

    @Incoming("in")
    public void consume(Domain in) {
        Log.infof("Consumer received %s", in);
        received.add(in);
    }

    List<Domain> received() {
        return received;
    }

}
