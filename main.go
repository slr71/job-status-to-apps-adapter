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
	"github.com/cyverse-de/logcabin"
	"github.com/cyverse-de/version"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
)

// JobStatusUpdate contains the data POSTed to the apps service.
type JobStatusUpdate struct {
	UUID string `json:"uuid"`
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
func (p *Propagator) Propagate(uuid string) error {
	jsu := JobStatusUpdate{
		UUID: uuid,
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

func main() {
	var (
		cfgPath     = flag.String("config", "", "Path to the config file. Required.")
		showVersion = flag.Bool("version", false, "Print the version information")
		dbURI       = flag.String("db", "", "The URI used to connect to the database")
		maxRetries  = flag.Int64("retries", 3, "The maximum number of propagation retries to make")
		batchSize   = flag.Int("batch-size", 1000, "The number of concurrent jobs to process.")
		err         error
		cfg         *viper.Viper
		db          *sql.DB
		appsURI     string
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

					if err = proper.Propagate(jobExtID); err != nil {
						logcabin.Error.Print(err)
					}

				}(db, *maxRetries, appsURI, jobExtID)
			}

			wg.Wait()
		}
	}
}
