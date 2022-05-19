package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/turbine/config", turbineConfigHandler)
	http.HandleFunc("/order_events", orderEventsHandler)
	http.HandleFunc("/shipping_events", shippingEventsHandler)
	log.Println("payment-service is available at localhost:3002")
	log.Fatal(http.ListenAndServe(":3002", nil))
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
		log.Printf("Shipping %v failed, refunding payment...", shippingEvent)
		return
	}
}

func orderEventsHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var orderEvent OrderEvent
	json.Unmarshal(body, &orderEvent)
	fmt.Printf("Received pubsub events: %v\n", orderEvent)
	if !orderEvent.Status {
		return
	}
	rand.Seed(time.Now().UnixNano())
	publishMessage("payment_updates", PaymentEvent{
		BillingId:     1,
		Amount:        22,
		CorrelationId: orderEvent.CorrelationId,
		Status:        true,
	})
	w.Write([]byte("success"))
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

func createPaymentHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(fmt.Sprintf(`{"message": "paid"}`)))
}

func turbineConfigHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{
		"serviceName": "payment-service",
		"subscriptions": [
			{
				"topic":  "shipping_updates",
				"endpoint": "/shipping_events"
			},
			{
				"topic":  "order_updates",
				"endpoint": "/order_events"
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

type PaymentEvent struct {
	BillingId     int64   `json:"billing_id"`
	Amount        float64 `json:"amount"`
	CorrelationId string  `json:"correlation_id"`
	Status        bool    `json:"status"`
}

type ShippingEvent struct {
	Address       string `json:"address"`
	CorrelationId string `json:"correlation_id"`
	Status        bool   `json:"status"`
}
