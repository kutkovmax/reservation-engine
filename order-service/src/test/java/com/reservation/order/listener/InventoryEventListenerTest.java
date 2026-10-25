package com.reservation.order.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.reservation.order.event.InventoryReservationFailedEvent;
import com.reservation.order.event.InventoryReservedEvent;
import com.reservation.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class InventoryEventListenerTest {

    @Mock
    private OrderService orderService;

    private InventoryEventListener listener;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @BeforeEach
    void setUp() {
        listener = new InventoryEventListener(orderService, objectMapper);
    }

    @Test
    void routesInventoryReservedEventToService() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String json = """
            {
              "eventId": "%s",
              "eventType": "InventoryReserved",
              "eventVersion": 1,
              "causationId": "%s",
              "orderId": "%s",
              "sku": "BOOK-001",
              "quantity": 2,
              "occurredAt": "2026-11-01T12:30:01Z",
              "unknownField": "shouldBeIgnored"
            }
            """.formatted(eventId, UUID.randomUUID(), orderId);

        listener.handleInventoryEvent(json);

        ArgumentCaptor<InventoryReservedEvent> captor = ArgumentCaptor.forClass(InventoryReservedEvent.class);
        verify(orderService).handleInventoryReserved(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(eventId);
        assertThat(captor.getValue().orderId()).isEqualTo(orderId);
        assertThat(captor.getValue().sku()).isEqualTo("BOOK-001");
        assertThat(captor.getValue().quantity()).isEqualTo(2);
    }

    @Test
    void routesInventoryReservationFailedEventToService() {
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String json = """
            {
              "eventId": "%s",
              "eventType": "InventoryReservationFailed",
              "eventVersion": 1,
              "causationId": "%s",
              "orderId": "%s",
              "sku": "BOOK-001",
              "quantity": 2,
              "reason": "INSUFFICIENT_STOCK",
              "occurredAt": "2026-11-01T12:30:01Z"
            }
            """.formatted(eventId, UUID.randomUUID(), orderId);

        listener.handleInventoryEvent(json);

        ArgumentCaptor<InventoryReservationFailedEvent> captor = ArgumentCaptor.forClass(InventoryReservationFailedEvent.class);
        verify(orderService).handleInventoryReservationFailed(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(eventId);
        assertThat(captor.getValue().orderId()).isEqualTo(orderId);
        assertThat(captor.getValue().reason()).isEqualTo("INSUFFICIENT_STOCK");
    }

    @Test
    void ignoresUnknownEventTypeWithoutError() {
        String json = """
            {
              "eventId": "%s",
              "eventType": "UnknownEvent",
              "eventVersion": 1
            }
            """.formatted(UUID.randomUUID());

        listener.handleInventoryEvent(json);

        verifyNoInteractions(orderService);
    }
}
