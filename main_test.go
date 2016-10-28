package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/configurate"
	"github.com/johnworth/messaging"
	"github.com/spf13/viper"
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

	mock.ExpectBegin()

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

	if p.tx == nil {
		t.Error("transaction was nil")
	}

	if p.appsURI != "uri" {
		t.Errorf("appsURI was %s rather than 'uri'", p.appsURI)
	}
}

func TestFinished(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	err = p.Finished()
	if err != nil {
		t.Errorf("error calling Finished(): %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in Finished()")
	}
}

func TestFinishedWithRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}
	p.rollback = true

	err = p.Finished()
	if err != nil {
		t.Errorf("error calling Finished(): %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations in Finished()")
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

	mock.ExpectBegin()

	p, err := NewPropagator(db, server.URL)
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations from NewPropagator()")
	}

	n := time.Now()
	status := &DBJobStatusUpdate{
		Status:              string(messaging.SucceededState),
		ExternalID:          "external-id",
		Message:             "message",
		SentFrom:            "sent-from",
		SentFromHostname:    "sent-from-hostname",
		SentOn:              0,
		Propagated:          false,
		PropagationAttempts: 0,
		CreatedDate:         n,
	}

	err = p.Propagate(status)
	if err != nil {
		t.Errorf("error from Propagate(): %s", err)
	}

	actual := &JobStatusUpdateWrapper{}
	if err = json.Unmarshal(body, actual); err != nil {
		t.Errorf("error unmarshalling body: %s", err)
	}

	if actual.State.Status != status.Status {
		t.Errorf("status was %s instead of %s", actual.State.Status, status.Status)
	}

	if actual.State.UUID != status.ExternalID {
		t.Errorf("uuid field was %s instead of %s", actual.State.UUID, status.ExternalID)
	}
}

func TestJobUpdates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	now := time.Now()

	rows := sqlmock.NewRows([]string{
		"id",
		"external_id",
		"message",
		"status",
		"sent_from",
		"sent_from_hostname",
		"sent_on",
		"propagated",
		"propagation_attempts",
		"last_propagation_attempt",
		"created_date",
	}).AddRow(
		"id",
		"external-id",
		"message",
		"status",
		"sent-from",
		"sent-from-hostname",
		0,
		false,
		0,
		nil,
		now,
	)
	mock.ExpectBegin()
	mock.ExpectQuery("select id").
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

	if updates[0].Message != "message" {
		t.Errorf("message was %s instead of 'message'", updates[0].Message)
	}

	if updates[0].Status != "status" {
		t.Errorf("status was %s instead of 'status'", updates[0].Status)
	}

	if updates[0].SentFrom != "sent-from" {
		t.Errorf("sent from was %s instead of 'sent-from'", updates[0].SentFrom)
	}

	if updates[0].SentFromHostname != "sent-from-hostname" {
		t.Errorf("sent from hostname was %s instead of 'sent-from-hostname'", updates[0].SentFromHostname)
	}

	if updates[0].SentOn != 0 {
		t.Errorf("sent on was %d intead of 0", updates[0].SentOn)
	}

	if updates[0].Propagated {
		t.Error("propagated was true")
	}

	if updates[0].PropagationAttempts != 0 {
		t.Errorf("propagation attempts was %d instead of 0", updates[0].PropagationAttempts)
	}

	if updates[0].CreatedDate != now {
		t.Errorf("created date was set to %#v instead of %#v", updates[0].CreatedDate, now)
	}
}

func TestMarkPropagated(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE ONLY job_status_updates").
		WithArgs("1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = p.MarkPropagated("1"); err != nil {
		t.Errorf("error calling MarkPropagated(): %s", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations from MarkPropagated()")
	}
}

func TestLastPropagated(t *testing.T) {
	n := time.Now()
	updates := []DBJobStatusUpdate{
		{
			Status:              string(messaging.SucceededState),
			ExternalID:          "external-id",
			Message:             "message",
			SentFrom:            "sent-from",
			SentFromHostname:    "sent-from-hostname",
			SentOn:              0,
			Propagated:          true,
			PropagationAttempts: 0,
			CreatedDate:         n,
		},
		{
			Status:              string(messaging.SucceededState),
			ExternalID:          "external-id1",
			Message:             "message",
			SentFrom:            "sent-from",
			SentFromHostname:    "sent-from-hostname",
			SentOn:              1,
			Propagated:          true,
			PropagationAttempts: 0,
			CreatedDate:         n,
		},
		{
			Status:              string(messaging.SucceededState),
			ExternalID:          "external-id2",
			Message:             "message",
			SentFrom:            "sent-from",
			SentFromHostname:    "sent-from-hostname",
			SentOn:              2,
			Propagated:          true,
			PropagationAttempts: 0,
			CreatedDate:         n,
		},
	}

	l := LastPropagated(updates)
	if l != 2 {
		t.Errorf("index of last propagated update was %d instead of 2", l)
	}
}

type AnyInt64 struct{}

func (a AnyInt64) Match(v driver.Value) bool {
	_, ok := v.(int64)
	return ok
}

func TestStorePropagationAttempts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	n := time.Now()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE ONLY job_status_updates").
		WithArgs("id", 0, AnyInt64{}).
		WillReturnResult(sqlmock.NewResult(1, 1))

	update := &DBJobStatusUpdate{
		ID:                  "id",
		Status:              string(messaging.SucceededState),
		ExternalID:          "external-id",
		Message:             "message",
		SentFrom:            "sent-from",
		SentFromHostname:    "sent-from-hostname",
		SentOn:              0,
		Propagated:          true,
		PropagationAttempts: 0,
		CreatedDate:         n,
	}

	p, err := NewPropagator(db, "uri")
	if err != nil {
		t.Errorf("error calling NewPropagator(): %s", err)
	}

	if err = p.StorePropagationAttempts(update); err != nil {
		t.Errorf("error from StorePropagationAttempts(): %s", err)
	}
}

func TestScanAndPropagate(t *testing.T) {
	n := time.Now()
	updates := []DBJobStatusUpdate{
		{
			ID:                  "id",
			Status:              string(messaging.SucceededState),
			ExternalID:          "external-id",
			Message:             "message",
			SentFrom:            "sent-from",
			SentFromHostname:    "sent-from-hostname",
			SentOn:              0,
			Propagated:          false,
			PropagationAttempts: 0,
			CreatedDate:         n,
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

	actual := &JobStatusUpdateWrapper{}
	if err = json.Unmarshal(body, actual); err != nil {
		t.Errorf("error unmarshalling body: %s", err)
	}

	if actual.State.Status != updates[0].Status {
		t.Errorf("status was %s instead of %s", actual.State.Status, updates[0].Status)
	}

	if actual.State.UUID != updates[0].ExternalID {
		t.Errorf("uuid field was %s instead of %s", actual.State.UUID, updates[0].ExternalID)
	}
}

func TestScanAndPropagateWithServerError(t *testing.T) {
	n := time.Now()
	updates := []DBJobStatusUpdate{
		{
			ID:                  "id",
			Status:              string(messaging.SucceededState),
			ExternalID:          "external-id",
			Message:             "message",
			SentFrom:            "sent-from",
			SentFromHostname:    "sent-from-hostname",
			SentOn:              0,
			Propagated:          false,
			PropagationAttempts: 0,
			CreatedDate:         n,
		},
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error occurred creating the mock db: %s", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE ONLY job_status_updates").
		WithArgs("id", 1, AnyInt64{}).
		WillReturnResult(sqlmock.NewResult(1, 1))

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
