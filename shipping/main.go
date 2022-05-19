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
	http.HandleFunc("/payment_events", paymentEventsHandler)
	log.Println("shipping-service is available at localhost:3003")
	log.Fatal(http.ListenAndServe(":3003", nil))
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
		return
	}
	rand.Seed(time.Now().UnixNano())
	publishMessage("shipping_updates", ShippingEvent{
		Address:       "acme address",
		CorrelationId: paymentEvent.CorrelationId,
		Status:        rand.Intn(2) == 1,
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

func turbineConfigHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Add("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{
		"serviceName": "shipping-service",
		"subscriptions": [
			{
				"topic":  "payment_updates",
				"endpoint": "/payment_events"
			}
		]
	}`))
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
