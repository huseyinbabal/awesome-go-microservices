package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/turbine/config", turbineConfigHandler)
	http.HandleFunc("/orders", createOrderHandler)
	http.HandleFunc("/payment_events", paymentEventsHandler)
	http.HandleFunc("/shipping_events", shippingEventsHandler)

	log.Println("order-service is available at localhost:3001")
	log.Fatal(http.ListenAndServe(":3001", nil))
}

func shippingEventsHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var shippingEvent ShippingEvent
	json.Unmarshal(body, &shippingEvent)
	fmt.Printf("Received pubsub events: %v\n", shippingEvent)
	if !shippingEvent.Status {
		log.Printf("Shipping %v failed, updating order status as FAILED", shippingEvent)
		return
	}

}

func paymentEventsHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var paymentEvent PaymentEvent
	json.Unmarshal(body, &paymentEvent)
	fmt.Printf("Received pubsub events: %v\n", paymentEvent)
	if !paymentEvent.Status {
		log.Printf("Payment %v failed, updating order status as FAILED", paymentEvent)
		return
	}
}

func createOrderHandler(writer http.ResponseWriter, request *http.Request) {
	correlationId, _ := uuid.NewUUID()
	err := publishMessage("order_updates", OrderEvent{
		OrderId:       1,
		ProductId:     2,
		CorrelationId: correlationId.String(),
		Status:        true,
	})
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("error"))
		return
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(correlationId.String()))
}

func publishMessage(topic string, message interface{}) error {
	messageBytes, err := json.Marshal(message)
	if err != nil {
		return errors.New("couldn't publish message")
	}
	log.Printf("Publishing to the topic %s, message %v\n", topic, message)
	url := fmt.Sprintf("http://localhost:8466/v1/pubsub/%s/message", topic)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(messageBytes))
	if err != nil {
		log.Println("Couldn't connect to Turbine service: ", err)
		return errors.New("couldn't connect to Turbine service")
	}
	if resp.StatusCode == 200 {
		log.Println("Message published.")
	} else {
		log.Println("Failed to publish message, status code is ", resp.StatusCode)
	}
	return nil
}

func turbineConfigHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{
		"serviceName": "order-service",
		"subscriptions": [
			{
				"topic":  "shipping_updates",
				"endpoint": "/shipping_events"
			},
			{
				"topic":  "payment_updates",
				"endpoint": "/payment_events"
			}
		]
	}`))
}

type OrderEvent struct {
	OrderId       int64  `json:"order_id"`
	ProductId     int64  `json:"product_id"`
	CorrelationId string `json:"correlation_id"`
	Status        bool   `json:"status"`
}

type ShippingEvent struct {
	Address       string `json:"address"`
	CorrelationId string `json:"correlation_id"`
	Status        bool   `json:"status"`
}

type PaymentEvent struct {
	BillingId     int64   `json:"billing_id"`
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlation_id"`
	Status        bool    `json:"status"`
}
