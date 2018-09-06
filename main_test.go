package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-events/ping"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v2"
)

var (
	cfg *viper.Viper
)

func inittests(t *testing.T) {
	var err error
	cfg, err = configurate.InitDefaults("../test/test_config.yaml", configurate.JobServicesDefaults)
	if err != nil {
		t.Error(err)
	}
}

func TestUnpropagated(t *testing.T) {
	inittests(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was encountered when creating the mock database", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"external_id"}).AddRow("1")
	mock.ExpectQuery("select distinct external_id").
		WithArgs(1).
		WillReturnRows(rows)

	_, err = Unpropagated(db, 1)
	if err != nil {
		t.Errorf("error calling Unpropagated: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in Unpropagated()")
	}
}

func TestNewPropagator(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in NewPropagator()")
	}

	if p.db != db {
		t.Error("dbs did not match")
	}

	if p.appsURI != "uri" {
		t.Errorf("appsURI was %s rather than 'uri'", p.appsURI)
	}
}

func TestPropagate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("error reading body: %s", err)
		}
		fmt.Fprintln(w, "Hello")
	}))
	defer server.Close()

	p, err := NewPropagator(db, server.URL)
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations from NewPropagator()")
	}

	status := &DBJobStatusUpdate{
		ExternalID: "external-id",
	}

	err = p.Propagate(status)
	if err != nil {
		t.Errorf("error from Propagate(): %s", err)
	}

	actual := &JobStatusUpdate{}
	if err = json.Unmarshal(body, actual); err != nil {
		t.Errorf("error unmarshalling body: %s", err)
	}

	if actual.UUID != status.ExternalID {
		t.Errorf("uuid field was %s instead of %s", actual.UUID, status.ExternalID)
	}
}

func TestJobUpdates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"id",
		"external_id",
	}).AddRow(
		"id",
		"external-id",
	)
	mock.ExpectQuery("select distinct id").
		WithArgs("external-id", 1).
		WillReturnRows(rows)

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	updates, err := p.JobUpdates("external-id", 1)
	if err != nil {
		t.Errorf("error calling JobUpdates(): %s", err)
	}

	if len(updates) != 1 {
		t.Errorf("number of updates returned was not 1: %d", len(updates))
	}

	if mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations from JobUpdates()")
	}

	if updates[0].ID != "id" {
		t.Errorf("id was %s instead of 'id'", updates[0].ID)
	}

	if updates[0].ExternalID != "external-id" {
		t.Errorf("id was %s instead of 'external-id'", updates[0].ExternalID)
	}
}

func TestScanAndPropagate(t *testing.T) {
	updates := []DBJobStatusUpdate{
		{
			ID:         "id",
			ExternalID: "external-id",
		},
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()

	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("error reading body: %s", err)
		}
		fmt.Fprintln(w, "Hello")
	}))
	defer server.Close()

	p, err := NewPropagator(db, server.URL)
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = p.ScanAndPropagate(updates, 2); err != nil {
		t.Errorf("error from ScanAndPropagate(): %s", err)
	}

	actual := &JobStatusUpdate{}
	if err = json.Unmarshal(body, actual); err != nil {
		t.Errorf("error unmarshalling body: %s", err)
	}

	if actual.UUID != updates[0].ExternalID {
		t.Errorf("uuid field was %s instead of %s", actual.UUID, updates[0].ExternalID)
	}
}

func TestScanAndPropagateWithServerError(t *testing.T) {
	updates := []DBJobStatusUpdate{
		{
			ID:         "id",
			ExternalID: "external-id",
		},
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = p.ScanAndPropagate(updates, 2); err != nil {
		t.Errorf("error from ScanAndPropagate(): %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Error("unfulfilled expectations from ScanAndPropagate()")
	}
}

type MockConsumer struct {
	exchange     string
	exchangeType string
	queue        string
	key          string
	handler      messaging.MessageHandler
}

type MockMessage struct {
	key string
	msg []byte
}

type MockMessenger struct {
	consumers         []MockConsumer
	publishedMessages []MockMessage
	publishTo         []string
	publishError      bool
}

func (m *MockMessenger) Close()  {}
func (m *MockMessenger) Listen() {}

func (m *MockMessenger) AddConsumer(exchange, exchangeType, queue, key string, handler messaging.MessageHandler) {
	m.consumers = append(m.consumers, MockConsumer{
		exchange:     exchange,
		exchangeType: exchangeType,
		queue:        queue,
		key:          key,
		handler:      handler,
	})
}

func (m *MockMessenger) Publish(key string, msg []byte) error {
	if m.publishError {
		return errors.New("publish error")
	}
	m.publishedMessages = append(m.publishedMessages, MockMessage{key: key, msg: msg})
	return nil
}

func (m *MockMessenger) SetupPublishing(exchange string) error {
	m.publishTo = append(m.publishTo, exchange)
	return nil
}

func TestNewEventHandler(t *testing.T) {
	client := &MockMessenger{}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}
	if handler.client != client {
		t.Errorf("client was %#v instead of %#v", handler.client, client)
	}
	if handler.exchange != "exchange" {
		t.Errorf("handler.exchange was %s instead of 'exchange'", handler.exchange)
	}
	if handler.exchangeType != "exchange-type" {
		t.Errorf("handler.exchangeType was %s instead of 'exchange-type'", handler.exchangeType)
	}
	if handler.queueName != "queue-name" {
		t.Errorf("handler.queueName was %s instead of 'queue-name'", handler.queueName)
	}
	if handler.listenKey != "listen-key" {
		t.Errorf("handler.listenKey was %s instead of 'listen-key'", handler.listenKey)
	}
}

func TestEventHandlerInit(t *testing.T) {
	client := &MockMessenger{
		consumers: make([]MockConsumer, 0),
		publishTo: make([]string, 0),
	}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}

	handler.Init()

	mm := handler.client.(*MockMessenger)
	numPublishTo := len(mm.publishTo)
	if numPublishTo != 1 {
		t.Errorf("numPublishTo was %d instead of 1", numPublishTo)
	}
	if mm.publishTo[0] != "exchange" {
		t.Errorf("publishTo was %s instead of 'exchange'", mm.publishTo[0])
	}
	numConsumers := len(mm.consumers)
	if numConsumers != 1 {
		t.Errorf("numConsumers was %d instead of 1", numConsumers)
	}
	if mm.consumers[0].exchange != "exchange" {
		t.Errorf("consumer exchange was %s instead of 'exchange'", mm.consumers[0].exchange)
	}
	if mm.consumers[0].exchangeType != "exchange-type" {
		t.Errorf("consumer exchange type was %s instead of 'exchange-type'", mm.consumers[0].exchangeType)
	}
}

func TestPing(t *testing.T) {
	client := &MockMessenger{
		publishedMessages: make([]MockMessage, 0),
	}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}
	handler.Ping(amqp.Delivery{})
	mm := handler.client.(*MockMessenger)

	numMessages := len(mm.publishedMessages)
	if numMessages != 1 {
		t.Errorf("numMessages was %d instead of 1", numMessages)
	}
	pong := &ping.Pong{}
	if err := json.Unmarshal(mm.publishedMessages[0].msg, pong); err != nil {
		t.Errorf("error unmarshalling message: %s", err)
	}
}

func TestRoutePing(t *testing.T) {
	client := &MockMessenger{
		publishedMessages: make([]MockMessage, 0),
	}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}
	d := amqp.Delivery{
		RoutingKey: pingKey,
	}
	handler.Route(d)
	mm := handler.client.(*MockMessenger)

	numMessages := len(mm.publishedMessages)
	if numMessages != 1 {
		t.Errorf("numMessages was %d instead of 1", numMessages)
	}
	pong := &ping.Pong{}
	if err := json.Unmarshal(mm.publishedMessages[0].msg, pong); err != nil {
		t.Errorf("error unmarshalling message: %s", err)
	}
}

func TestRoutePong(t *testing.T) {
	client := &MockMessenger{
		publishedMessages: make([]MockMessage, 0),
	}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}
	d := amqp.Delivery{
		RoutingKey: pongKey,
	}
	handler.Route(d)
	mm := handler.client.(*MockMessenger)
	numMessages := len(mm.publishedMessages)
	if numMessages != 0 {
		t.Errorf("numMessages was %d instead of 0", numMessages)
	}
}

func TestRouteUnknown(t *testing.T) {
	client := &MockMessenger{
		publishedMessages: make([]MockMessage, 0),
	}
	handler := NewEventHandler(client, "exchange", "exchange-type", "queue-name", "listen-key")
	if handler == nil {
		t.Error("handler was nil")
	}
	d := amqp.Delivery{
		RoutingKey: "unknown",
	}
	handler.Route(d)
	mm := handler.client.(*MockMessenger)
	numMessages := len(mm.publishedMessages)
	if numMessages != 0 {
		t.Errorf("numMessages was %d instead of 0", numMessages)
	}
}
