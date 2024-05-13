package ubatch_test

import (
	"errors"
	"testing"
	"ubatch"
)

type MockJob struct {
	data string
}

func (j MockJob) Do() (interface{}, error) {
	if j.data == "1" {
		return "", errors.New("my error")
	}

	return j.data, nil
}

type MockBatchProcessor struct{}

func (c *MockBatchProcessor) Process(jobs []ubatch.Job) []ubatch.JobResult {
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

func TestNewBatcher(t *testing.T) {
	_, err := ubatch.NewBatcher[MockJob](
		&MockBatchProcessor{},
		ubatch.WithSize(batchSize),
		ubatch.WithTimeout(maxBatchDuration))
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestNewBatcherSizeError(t *testing.T) {
	_, err := ubatch.NewBatcher[MockJob](
		&MockBatchProcessor{},
		ubatch.WithTimeout(maxBatchDuration))
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestNewBatcherDurationError(t *testing.T) {
	_, err := ubatch.NewBatcher[MockJob](
		&MockBatchProcessor{},
		ubatch.WithSize(batchSize))
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestSubmitSuccess(t *testing.T) {
	batcher, _ := ubatch.NewBatcher[MockJob](
		&MockBatchProcessor{},
		ubatch.WithSize(batchSize),
		ubatch.WithTimeout(maxBatchDuration))

	batcher.Run()

	job := MockJob{data: "0"}

	res := batcher.Submit(job)

	if res.Result.(string) != "0" {
		t.Errorf("expected '0', got %v and error %v", res.Result, res.Err)
	}
}
func TestSubmitFailed(t *testing.T) {
	batcher, _ := ubatch.NewBatcher[MockJob](
		&MockBatchProcessor{},
		ubatch.WithSize(batchSize),
		ubatch.WithTimeout(maxBatchDuration))

	batcher.Run()

	job := MockJob{data: "1"}

	res := batcher.Submit(job)

	if res.Result.(string) != "" {
		t.Errorf("expected blank, got result %v and error %v", res.Result, res.Err)
	}
}
