package com.reservation.order.controller;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.event.OrderPlacedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private OrderController orderController;

    @Test
    void usesSkuAsKafkaPartitionKey() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);

        orderController.sendTestOrder(request);

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("order-events"), eq("BOOK-001"), eventCaptor.capture());

        assertThat(eventCaptor.getValue())
            .isInstanceOfSatisfying(OrderPlacedEvent.class, event -> {
                assertThat(event.sku()).isEqualTo("BOOK-001");
                assertThat(event.quantity()).isEqualTo(2);
            });
    }
}
