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
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-events/jobevents"
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
const eventBase = "events.job-status-to-apps-adapter.%s"

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

// EventEmitter is a function that should send an event to the AMQP broker.
type EventEmitter func(event, service string, update *DBJobStatusUpdate) error

// JobStatusUpdate contains the data POSTed to the apps service.
type JobStatusUpdate struct {
	Status         string `json:"status"`
	CompletionDate string `json:"completion_date,omitempty"`
	UUID           string `json:"uuid"`
}

// JobStatusUpdateWrapper wraps a JobStatusUpdate
type JobStatusUpdateWrapper struct {
	State JobStatusUpdate `json:"state"`
}

// DBJobStatusUpdate represents a row from the job_status_updates table
type DBJobStatusUpdate struct {
	ID                     string
	ExternalID             string
	Message                string
	Status                 string
	SentFrom               string
	SentFromHostname       string
	SentOn                 int64
	Propagated             bool
	PropagationAttempts    int64
	LastPropagationAttempt sql.NullInt64
	CreatedDate            time.Time
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
	emit    EventEmitter
	appsURI string
}

// NewPropagator returns a *Propagator that has been initialized with a new
// transaction.
func NewPropagator(d *sql.DB, appsURI string, emit EventEmitter) (*Propagator, error) {
	var err error
	if err != nil {
		return nil, err
	}
	return &Propagator{
		db:      d,
		appsURI: appsURI,
		emit:    emit,
	}, nil
}

// Propagate pushes the update to the apps service.
func (p *Propagator) Propagate(status *DBJobStatusUpdate) error {
	jsu := JobStatusUpdate{
		Status: status.Status,
		UUID:   status.ExternalID,
	}

	if jsu.Status == string(messaging.SucceededState) || jsu.Status == string(messaging.FailedState) {
		jsu.CompletionDate = fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond))
	}

	jsuw := JobStatusUpdateWrapper{
		State: jsu,
	}

	logcabin.Info.Printf("Job status in the propagate function for job %s is: %#v", jsu.UUID, jsuw)
	msg, err := json.Marshal(jsuw)
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
		p.emit("propagate-failed", fmt.Sprintf("error propagating job %s: %s", status.ExternalID, err), status)
		logcabin.Error.Printf("Error sending job status to %s in the propagate function for job %s: %#v", p.appsURI, jsu.UUID, err)
		return err
	}
	defer resp.Body.Close()

	p.emit("propagate", fmt.Sprintf("sent job %s to apps", status.ExternalID), status)

	logcabin.Info.Printf("Response from %s in the propagate function for job %s is: %s", p.appsURI, jsu.UUID, resp.Status)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		p.emit(
			"propagate-failed",
			fmt.Sprintf("error propagating job %s: HTTP status code %d from apps", status.ExternalID, resp.StatusCode),
			status,
		)
		return errors.New("bad response")
	}

	return nil
}

// JobUpdates returns a list of JobUpdate's which haven't exceeded their
// retries, sorted by their SentOn field.
func (p *Propagator) JobUpdates(extID string, maxRetries int64) ([]DBJobStatusUpdate, error) {
	queryStr := `
	select id,
				 external_id,
				 message,
				 status,
				 sent_from,
				 sent_from_hostname,
				 sent_on,
				 propagated,
				 propagation_attempts,
				 last_propagation_attempt,
				 created_date
	  from job_status_updates
	 where external_id = $1
	   and propagation_attempts < $2
	order by sent_on asc`
	rows, err := p.db.Query(queryStr, extID, maxRetries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var retval []DBJobStatusUpdate
	for rows.Next() {
		r := DBJobStatusUpdate{}
		err = rows.Scan(
			&r.ID,
			&r.ExternalID,
			&r.Message,
			&r.Status,
			&r.SentFrom,
			&r.SentFromHostname,
			&r.SentOn,
			&r.Propagated,
			&r.PropagationAttempts,
			&r.LastPropagationAttempt,
			&r.CreatedDate,
		)
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
		tx.Rollback()
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

// StorePropagationAttempts stores an incremented value for the update's
// propagation_attempts field.
func (p *Propagator) StorePropagationAttempts(update *DBJobStatusUpdate) error {
	newVal := update.PropagationAttempts
	id := update.ID
	lastAttemptTime := time.Now().UnixNano() / int64(time.Millisecond)
	insertStr := `UPDATE ONLY job_status_updates SET propagation_attempts = $2, last_propagation_attempt = $3 WHERE id = $1`
	tx, err := p.db.Begin()
	if err != nil {
		tx.Rollback()
		return err
	}
	if _, err = tx.Exec(insertStr, id, newVal, lastAttemptTime); err != nil {
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
		if !subupdates.Propagated && subupdates.PropagationAttempts < maxRetries {
			logcabin.Info.Printf("Propagating %#v", subupdates)
			if err = p.Propagate(&subupdates); err != nil {
				logcabin.Error.Print(err)
				subupdates.PropagationAttempts = subupdates.PropagationAttempts + 1
				if err = p.StorePropagationAttempts(&subupdates); err != nil {
					logcabin.Error.Print(err)
				}
				continue
			}
			logcabin.Info.Printf("Marking update %s as propagated", subupdates.ID)
			if err = p.MarkPropagated(subupdates.ID); err != nil {
				logcabin.Error.Print(err)
				continue
			}
		}
	}
	return nil
}

// eventFromUpdate creates a *jobevents.JobEvent based on the DBJobStatusUpdate
// that is passed in.
func eventFromUpdate(event, service, message string, update *DBJobStatusUpdate) *jobevents.JobEvent {
	return &jobevents.JobEvent{
		EventName:   event,
		ServiceName: service,
		Message:     message,
		Host:        hostname(),
		JobId:       update.ExternalID,
		JobState:    update.Status,
		Timestamp:   time.Now().Unix(),
	}
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

// Emit emits an event over AMQP.
func (e *EventHandler) Emit(event, message string, update *DBJobStatusUpdate) error {
	ev := eventFromUpdate(event, "job-status-to-apps-adapter", message, update)
	j, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	eventKey := fmt.Sprintf(eventBase, event)
	return e.client.Publish(eventKey, j)
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

					proper, err := NewPropagator(db, appsURI, eventer.Emit)
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
