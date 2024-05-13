package ubatch

type Job interface {
	Do() (interface{}, error)
}

// JobResult represents the result of processing a Job.
type JobResult struct {
	Job    Job
	Result interface{}
	Err    error
}

// BatchProcessor defines the interface for processing jobs in batches.
type BatchProcessor interface {
	Process([]Job) []JobResult
}
