package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/configurate"
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

	err = p.Propagate("external-id")
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
