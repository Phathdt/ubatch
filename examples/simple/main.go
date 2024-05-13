package main

import (
	"fmt"
	"time"
	"ubatch"
)

const (
	batchSize        = 3
	maxBatchDuration = 3 * time.Second
	iterations       = 5
)

type Job struct {
	data string
}

func (j Job) Do() (interface{}, error) {
	return j.data, nil
}

type BatchProcessor struct{}

func (c *BatchProcessor) Process(jobs []ubatch.Job) []ubatch.JobResult {
	results := make([]ubatch.JobResult, len(jobs))

	for i, job := range jobs {
		res, err := job.Do()

		results[i] = ubatch.JobResult{
			Job:    job,
			Result: res,
			Err:    err,
		}
	}

	return results
}

func main() {
	batcher, err := ubatch.NewBatcher[Job](
		&BatchProcessor{},
		ubatch.WithSize(batchSize),
		ubatch.WithTimeout(maxBatchDuration))
	if err != nil {
		panic(err)
	}

	go batcher.Run()
	jobs := make([]Job, iterations)
	for i := 0; i < iterations; i++ {
		jobs[i] = Job{data: fmt.Sprintf("job %d", i)}
	}

	batcher.SubmitJobs(jobs)

	results := make([]ubatch.JobResult, iterations)
	for i := 0; i < iterations; i++ {
		results[i] = <-batcher.Results
	}

	fmt.Println("results", results)
	batcher.Shutdown()
}
