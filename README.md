# MicroBatching implement - uBatch

Micro-batching is a technique that divides the incoming data stream into small batches based on time or size, and processes each batch as a mini-batch job. Unlike pure stream processing, which processes each record individually and continuously, micro-batching allows for some latency and buffering between data ingestion and processing. However, unlike pure batch processing, which processes large batches of data periodically, micro-batching enables near-real-time processing and delivery of results.



## Usage

### Implement `Job`

```go
type Job struct {
  data string
}

func (j Job) Do() (interface{}, error) {
  // do something

  // return something else
  return j.data, nil
}
```

### Implement the `BatchProcessor`

```go
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
```

### Use the `Batcher`

```go

const (
	batchSize        = 3
	maxBatchDuration = 1 * time.Second
	iterations       = 5
)

func main() {
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
}
```
