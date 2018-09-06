// job-status-to-apps-adapter
//
// This service periodically queries the DE database's job-status-updates table
// for new entries and propagates them up through the apps services's API, which
// eventually triggers job notifications in the UI.
//
// This service works by first querying for all jobs that have unpropagated
// statuses, iterating through each job and propagating all unpropagated
// status in the correct order. It records each attempt and will not re-attempt
// a propagation if the number of retries exceeds the configured maximum number
// of retries (which defaults to 3).
//
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	_ "expvar"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-events/ping"
	"github.com/cyverse-de/logcabin"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v2"
)

const pingKey = "events.job-status-to-apps-adapter.ping"
const pongKey = "events.job-status-to-apps-adapter.pong"

// JobStatusUpdate contains the data POSTed to the apps service.
type JobStatusUpdate struct {
	UUID string `json:"uuid"`
}

// DBJobStatusUpdate represents a row from the job_status_updates table
type DBJobStatusUpdate struct {
	ExternalID string
}

// Unpropagated returns a []string of the UUIDs for jobs that have steps that
// haven't been propagated yet but haven't passed their retry limit.
func Unpropagated(d *sql.DB, maxRetries int64) ([]string, error) {
	queryStr := `
	select distinct external_id
	  from job_status_updates
	 where propagated = 'false'
	   and propagation_attempts < $1`
	rows, err := d.Query(queryStr, maxRetries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var retval []string
	for rows.Next() {
		var extID string
		err = rows.Scan(&extID)
		if err != nil {
			return nil, err
		}
		retval = append(retval, extID)
	}
	err = rows.Err()
	return retval, err
}

// Propagator looks for job status updates in the database and pushes them to
// the apps service if they haven't been successfully pushed there yet.
type Propagator struct {
	db      *sql.DB
	appsURI string
}

// NewPropagator returns a *Propagator that has been initialized with a new
// transaction.
func NewPropagator(d *sql.DB, appsURI string) (*Propagator, error) {
	var err error
	if err != nil {
		return nil, err
	}
	return &Propagator{
		db:      d,
		appsURI: appsURI,
	}, nil
}

// Propagate pushes the update to the apps service.
func (p *Propagator) Propagate(status *DBJobStatusUpdate) error {
	jsu := JobStatusUpdate{
		UUID: status.ExternalID,
	}

	logcabin.Info.Printf("Job status in the propagate function for job %s is: %#v", jsu.UUID, jsu)
	msg, err := json.Marshal(jsu)
	if err != nil {
		logcabin.Error.Print(err)
		return err
	}

	buf := bytes.NewBuffer(msg)
	if err != nil {
		logcabin.Error.Print(err)
		return err
	}

	logcabin.Info.Printf("Message to propagate: %s", string(msg))

	logcabin.Info.Printf("Sending job status to %s in the propagate function for job %s", p.appsURI, jsu.UUID)
	resp, err := http.Post(p.appsURI, "application/json", buf)
	if err != nil {
		logcabin.Error.Printf("Error sending job status to %s in the propagate function for job %s: %#v", p.appsURI, jsu.UUID, err)
		return err
	}
	defer resp.Body.Close()

	logcabin.Info.Printf("Response from %s in the propagate function for job %s is: %s", p.appsURI, jsu.UUID, resp.Status)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errors.New("bad response")
	}

	return nil
}

// JobUpdates returns a list of JobUpdate's which haven't exceeded their
// retries, sorted by their SentOn field.
func (p *Propagator) JobUpdates(extID string, maxRetries int64) ([]DBJobStatusUpdate, error) {
	queryStr := `
  select distinct external_id
    from job_status_updates
   where external_id = $1
     and propagation_attempts < $2
     and propagated = false`
	rows, err := p.db.Query(queryStr, extID, maxRetries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var retval []DBJobStatusUpdate
	for rows.Next() {
		r := DBJobStatusUpdate{}
		err = rows.Scan(&r.ExternalID)
		if err != nil {
			return nil, err
		}
		retval = append(retval, r)
	}
	err = rows.Err()
	return retval, err
}

// MarkPropagated marks the job as propagated in the database as part of the
// transaction tracked by the *Propagator.
func (p *Propagator) MarkPropagated(id string) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	updateStr := `UPDATE ONLY job_status_updates SET propagated = 'true' where id = $1`
	if _, err = tx.Exec(updateStr, id); err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

// ScanAndPropagate marks any unpropagated updates that appear __before__ a
// propagated update as propagated.
func (p *Propagator) ScanAndPropagate(updates []DBJobStatusUpdate, maxRetries int64) error {
	var err error
	for _, subupdates := range updates {
		logcabin.Info.Printf("Propagating %#v", subupdates)
		if err = p.Propagate(&subupdates); err != nil {
			logcabin.Error.Print(err)
			continue
		}
	}
	return nil
}

// Messenger defines an interface for handling AMQP operations. This is the
// subset of functionality needed by job-status-to-apps-adapter.
type Messenger interface {
	AddConsumer(string, string, string, string, messaging.MessageHandler)
	Close()
	Listen()
	Publish(string, []byte) error
	SetupPublishing(string) error
}

// EventHandler processes incoming and outgoing event messages.
type EventHandler struct {
	client                                       Messenger
	exchange, exchangeType, queueName, listenKey string
}

// NewEventHandler returns a newly initialized and configured *EventHandler.
func NewEventHandler(client Messenger, exchange, exchangeType, queueName, listenKey string) *EventHandler {
	handler := &EventHandler{
		client:       client,
		exchange:     exchange,
		exchangeType: exchangeType,
		queueName:    queueName,
		listenKey:    listenKey,
	}
	go handler.client.Listen()
	return handler
}

// Init sets up AMQP publishing and adds the Route function as message handler.
func (e *EventHandler) Init() {
	e.client.SetupPublishing(e.exchange)
	e.client.AddConsumer(
		e.exchange,
		e.exchangeType,
		e.queueName,
		e.listenKey,
		e.Route,
	)
}

// Route delegates the handling of incoming event messages to the appropriate
// handler.
func (e *EventHandler) Route(delivery amqp.Delivery) {
	if err := delivery.Ack(false); err != nil {
		logcabin.Error.Print(err)
	}

	switch delivery.RoutingKey {
	case pingKey:
		e.Ping(delivery)
	case pongKey:
	default:
		logcabin.Error.Printf("unknown event received with key %s", delivery.RoutingKey)
	}
}

// Ping handles incoming ping requests.
func (e *EventHandler) Ping(delivery amqp.Delivery) {
	logcabin.Info.Println("Received ping")

	out, err := json.Marshal(&ping.Pong{})
	if err != nil {
		logcabin.Error.Print(err)
	}

	logcabin.Info.Println("Sent pong")

	if err = e.client.Publish(pongKey, out); err != nil {
		logcabin.Error.Print(err)
	}
}

func main() {
	var (
		cfgPath          = flag.String("config", "", "Path to the config file. Required.")
		showVersion      = flag.Bool("version", false, "Print the version information")
		dbURI            = flag.String("db", "", "The URI used to connect to the database")
		maxRetries       = flag.Int64("retries", 3, "The maximum number of propagation retries to make")
		eventsQueue      = flag.String("events-queue", "job_status_to_apps_adapter_events", "The AMQP queue name for job-status-to-apps-adapter events")
		eventsRoutingKey = flag.String("events-key", "events.job-status-to-apps-adapter.*", "The routing key to use to listen for events")
		batchSize        = flag.Int("batch-size", 1000, "The number of concurrent jobs to process.")
		err              error
		cfg              *viper.Viper
		db               *sql.DB
		appsURI          string
	)

	flag.Parse()

	logcabin.Init("job-status-to-apps-adapter", "job-status-to-apps-adapter")

	if *showVersion {
		version.AppVersion()
		os.Exit(0)
	}

	if *cfgPath == "" {
		fmt.Println("Error: --config must be set.")
		flag.PrintDefaults()
		os.Exit(-1)
	}

	cfg, err = configurate.InitDefaults(*cfgPath, configurate.JobServicesDefaults)
	if err != nil {
		logcabin.Error.Print(err)
		os.Exit(-1)
	}

	logcabin.Info.Println("Done reading config.")

	if *dbURI == "" {
		*dbURI = cfg.GetString("db.uri")
	} else {
		cfg.Set("db.uri", *dbURI)
	}

	appsURI = cfg.GetString("apps.callbacks_uri")
	amqpURI := cfg.GetString("amqp.uri")
	exchangeName := cfg.GetString("amqp.exchange.name")
	exchangeType := cfg.GetString("amqp.exchange.type")

	client, err := messaging.NewClient(amqpURI, false)
	if err != nil {
		logcabin.Error.Fatal(err)
	}

	eventer := NewEventHandler(
		client,
		exchangeName,
		exchangeType,
		*eventsQueue,
		*eventsRoutingKey,
	)
	if err != nil {
		logcabin.Error.Fatal(err)
	}

	eventer.Init()

	logcabin.Info.Println("Connecting to the database...")
	db, err = sql.Open("postgres", *dbURI)
	if err != nil {
		logcabin.Error.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		logcabin.Error.Fatal(err)
	}
	logcabin.Info.Println("Connected to the database")

	go func() {
		sock, err := net.Listen("tcp", "0.0.0.0:60000")
		if err != nil {
			logcabin.Error.Fatal(err)
		}
		http.Serve(sock, nil)
	}()

	for {
		var batches [][]string
		var wg sync.WaitGroup

		unpropped, err := Unpropagated(db, *maxRetries)
		if err != nil {
			logcabin.Error.Fatal(err)
		}

		for *batchSize < len(unpropped) {
			unpropped, batches = unpropped[*batchSize:], append(batches, unpropped[0:*batchSize])
		}
		batches = append(batches, unpropped)

		for _, batch := range batches {
			for _, jobExtID := range batch {
				wg.Add(1)

				go func(db *sql.DB, maxRetries int64, appsURI string, jobExtID string) {
					defer wg.Done()

					proper, err := NewPropagator(db, appsURI)
					if err != nil {
						logcabin.Error.Print(err)
					}

					updates, err := proper.JobUpdates(jobExtID, maxRetries)
					if err != nil {
						logcabin.Error.Print(err)
					}

					if err = proper.ScanAndPropagate(updates, maxRetries); err != nil {
						logcabin.Error.Print(err)
					}
				}(db, *maxRetries, appsURI, jobExtID)
			}

			wg.Wait()
		}
	}
}
