package ubatch

import (
	"errors"
	"sync"
	"time"
)

type Batcher[J Job] struct {
	jobs chan J

	results   chan JobResult
	processor BatchProcessor

	//batchSize is the maximum requests per batch
	batchSize int

	//batchTimeout is maximum time a batch can wait per batch before process
	batchTimeout time.Duration

	//for shutdown
	quit chan int
	wg   sync.WaitGroup
}

func NewBatcher[J Job](processor BatchProcessor, opts ...Option) (*Batcher[J], error) {
	if processor == nil {
		return nil, ErrNoProcessor
	}

	var option options
	for _, opt := range opts {
		opt.apply(&option)
	}

	if option.size <= 0 {
		return nil, ErrSize
	}

	if option.timeout <= 0 {
		return nil, ErrTimeout
	}

	b := &Batcher[J]{
		jobs:         make(chan J),
		results:      make(chan JobResult),
		processor:    processor,
		batchSize:    option.size,
		batchTimeout: option.timeout,
		quit:         make(chan int),
	}

	return b, nil
}

func (b *Batcher[J]) Submit(job J) JobResult {
	res := b.processor.Process([]Job{job})

	return res[0]
}

func (b *Batcher[J]) SubmitJobs(jobs []J) {
	go func(jobs []J) {
		for _, job := range jobs {
			b.jobs <- job
			b.wg.Add(1)
		}
	}(jobs)
}

func (b *Batcher[J]) Run() {
	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		batchJob := make([]Job, 0, b.batchSize)

		ticker := time.NewTicker(b.batchTimeout)
		for {
			select {
			case <-b.quit:
				ticker.Stop()

				if len(batchJob) > 0 {
					b.processBatch(batchJob)
				}
				return

			case job := <-b.jobs:
				batchJob = append(batchJob, job)

				if len(batchJob) == b.batchSize {
					b.processBatch(batchJob)

					batchJob = make([]Job, 0, b.batchSize)

					ticker.Reset(b.batchTimeout)
				}

			case <-ticker.C:
				if len(batchJob) > 0 {
					b.processBatch(batchJob)

					batchJob = make([]Job, 0, b.batchSize)
				}

			}
		}
	}()
}

func (b *Batcher[J]) processBatch(batch []Job) {
	results := b.processor.Process(batch)

	for _, result := range results {
		b.results <- result
	}

	for _ = range batch {
		b.wg.Done()
	}
}

// Shutdown waits for all submitted jobs to be processed before returning.
func (b *Batcher[J]) Shutdown() {
	close(b.quit)
	b.wg.Wait()
	close(b.jobs)
	close(b.results)
}

func (b *Batcher[J]) GetResults() <-chan JobResult {
	return b.results
}

var (
	ErrNoProcessor = errors.New("no processor")
	ErrSize        = errors.New("size should be greater than zero")
	ErrTimeout     = errors.New("timeout should be greater than zero")
)

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

type options struct {
	size    int
	timeout time.Duration
}

type Option interface {
	apply(opts *options)
}

// WithSize config size of batch
func WithSize(size int) Option {
	return sizeOption{size: size}
}

type sizeOption struct {
	size int
}

func (o sizeOption) apply(opts *options) {
	opts.size = o.size
}

// WithTimeout config timeout.
func WithTimeout(timeout time.Duration) Option {
	return timeoutOption{timeout: timeout}
}

type timeoutOption struct {
	timeout time.Duration
}

func (o timeoutOption) apply(opts *options) {
	opts.timeout = o.timeout
}
