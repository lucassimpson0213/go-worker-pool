package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"sync"
	"time"
)

type Handler func(ctx context.Context, j *Job, job_attempt_number int64) error

type RetryFunc Handler

type Job struct {
	ID            string
	Handler       Handler
	Timeout       time.Duration
	TimeOfEnqueue time.Time
	JobId int64
}

func connect_to_db() *sql.DB {
	connStr := "user=lucassimpson dbname=lucassimpson sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	return db

}

func worker(db *sql.DB, cont context.Context, id int, jobs <-chan Job, wg *sync.WaitGroup, deadLetterQueue chan<- string, ticker *time.Ticker) {

	for j := range jobs {
		ctx, cancel := context.WithTimeout(cont, j.Timeout)

		select {
		case <-cont.Done():
			wg.Done()
			cancel()
			return
		case <-ticker.C:
		}

		var job_attempt int64 = 0

		started_at := time.Now()

		db.Query("UPDATE Job Set started_at = $1 WHERE job_id = $", started_at, j.JobId)
		
		
		err := j.Handler(ctx, &j, job_attempt)
		cancel()
		job_attempt = job_attempt + 1
		db.ExecContext(ctx, "INSERT into Job_Attempts (job_id, attempt_number) VALUES ($1, $2)", j.JobId, job_attempt)

		if err == nil {
			time_end := time.Now()
			_, e := db.ExecContext(ctx, "UPDATE Job SET ended_at = $1 WHERE job_id = $2", time_end, j.JobId)

			if e != nil {
				log.Printf("job ended and there was an error with the database: ", e, j.JobId)
			}
			cancel()
			time.Sleep(1 * time.Second)
			wg.Done()
			continue
			

		}
		
			retryMessage := retry(cont, RetryFunc(j.Handler), &j, 5, j.JobId)

			if retryMessage == "timeout" || retryMessage == "max number of retries" {
				deadLetterQueue <- retryMessage
			}

		time.Sleep(1000)

		wg.Done()

	}
}

func retry(ctx context.Context, f RetryFunc, job *Job, numberOfRetries int, job_attempt int64) string {
	for i := 0; i < numberOfRetries; i++ {
		//fresh context for retry

		newctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := f(newctx, job, job_attempt)
		job_attempt = job_attempt + 1

		cancel()

		if err == nil {

			return "success"
		}

		select {
		case <-ctx.Done():
			return "timeout"
		default:
		}

	}

	return "max number of retries"

}

func main() {
	//global rate limiter so that workers can catch up
	gate := time.NewTicker(500 * time.Millisecond)
	defer gate.Stop()

	db := connect_to_db()
	if db == nil {
		fmt.Println("connection to db was not successful")
	}
	defer db.Close()

	jobs := make(chan Job, 5)

	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	//dead letter queue for jobs that have failed
	dlq := make(chan string, 128)

	//start workers first so each worker has a chance to get something from the channel
	for i := 0; i < 5; i++ {
		go worker(db, ctx, i, jobs, &wg, dlq, gate)
	}

	doWork := func(ctx context.Context, j *Job, job_attempt int64) error {

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
			return nil // pretend success
		}
	}

	//enqueue jobs
	for i := 0; i < 5; i++ {
		wg.Add(1)
		current_time := time.Now()

		var jobID int64
		fmt.Println("enqueued job at ", current_time)
		 error := db.QueryRow("INSERT INTO JOB (enqueued_at) VALUES ($1) RETURNING job_id", current_time).Scan(&jobID)


		if error != nil {
			log.Fatal("fatal")
		}

		fmt.Printf("Inserted job with ID %d\n", jobID)
		
		

		jobs <- Job{ID: strconv.Itoa(i), Handler: doWork, Timeout: 2 * time.Second, TimeOfEnqueue: current_time, JobId: jobID}

		//add goroutines to waitgroup
	}

	close(jobs)
	wg.Wait()
	close(dlq)

}
