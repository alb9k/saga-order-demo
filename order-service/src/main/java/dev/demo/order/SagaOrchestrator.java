package dev.demo.order;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Service
public class SagaOrchestrator {
    private final OrderRepository repo;
    private final RestClient client;
    private final KafkaTemplate<String, String> kafka;
    @Value("${services.payment:http://localhost:8082}")
    String payment;
    @Value("${services.inventory:http://localhost:8083}")
    String inventory;
    @Value("${services.shipping:http://localhost:8084}")
    String shipping;

    public SagaOrchestrator(OrderRepository repo, RestClient.Builder builder, KafkaTemplate<String, String> kafka) {
        this.repo = repo;
        this.client = builder.build();
        this.kafka = kafka;
    }

    public void start(UUID id) {
        var order = repo.findById(id).orElseThrow();
        List<String> completed = new ArrayList<>();
        try {
            step(payment, "payment", "authorize", id, order.failureStep);
            completed.add("payment");
            step(inventory, "inventory", "reserve", id, order.failureStep);
            completed.add("inventory");
            step(shipping, "shipping", "create", id, order.failureStep);
            completed.add("shipping");
            order.status = "COMPLETED";
            order.details = "All steps completed";
        } catch (Exception ex) {
            List<String> compensationFailures = new ArrayList<>();
            Collections.reverse(completed);
            for (String s : completed) {
                try {
                    compensate(s, id);
                } catch (Exception c) {
                    compensationFailures.add(s + ": " + c.getMessage());
                }
            }
            order.status = compensationFailures.isEmpty() ? "CANCELLED" : "COMPENSATION_FAILED";
            order.details = "Saga failed: " + ex.getMessage() + "; compensation failures=" + compensationFailures;
        }
        repo.save(order);
        // Kafka is used for best-effort lifecycle notification in this educational baseline.
        // This is NOT an atomic outbox; see README for production hardening.
        try {
            kafka.send("saga-order-events", id.toString(), id + "|" + order.status);
        } catch (Exception ignored) {
        }
    }

    private void step(String host, String service, String action, UUID id, String failAt) {
        client.post().uri(host + "/internal/" + service + "/" + id + "/" + action + "?fail=" + service.equals(failAt)).retrieve().toBodilessEntity();
    }

    private void compensate(String service, UUID id) {
        String host = switch (service) {
            case "payment" -> payment;
            case "inventory" -> inventory;
            default -> shipping;
        };
        String action = switch (service) {
            case "payment" -> "void";
            case "inventory" -> "release";
            default -> "cancel";
        };
        client.post().uri(host + "/internal/" + service + "/" + id + "/" + action).retrieve().toBodilessEntity();
    }
}
