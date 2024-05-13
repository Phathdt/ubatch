package ubatch_test

import (
	"fmt"
	"testing"
	"time"
	"ubatch"
)

const (
	batchSize        = 3
	maxBatchDuration = 1 * time.Second
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

func TestExample(t *testing.T) {
	batcher, err := ubatch.NewBatcher[Job](
		&BatchProcessor{},
		ubatch.WithSize(batchSize),
		ubatch.WithTimeout(maxBatchDuration))
	if err != nil {
		panic(err)
	}

	batcher.Run()
	jobs := make([]Job, iterations)
	for i := 0; i < iterations; i++ {
		jobs[i] = Job{data: fmt.Sprintf("job %d", i)}
	}

	batcher.SubmitJobs(jobs)

	results := make([]ubatch.JobResult, 0)
	resultChan := batcher.GetResults()
	for i := 0; i < iterations; i++ {
		result := <-resultChan
		results = append(results, result)
	}

	batcher.Shutdown()

	if len(results) != iterations {
		t.Errorf("expected results have %d element , got %d", iterations, len(results))
	}
}
