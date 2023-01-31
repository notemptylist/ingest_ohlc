package worker

import (
	"fmt"

	"github.com/notemptylist/ingest_ohlc/parser"
)

type Job struct {
	Id    int
	Fname string
}

type Result struct {
	Job    Job
	Status bool
	Result string
}

type Worker interface {
	Work(int, <-chan Job, chan<- Result)
}

type DefaultWorker struct {
}

func NewDefaultWorker() (*DefaultWorker, error) {
	d := DefaultWorker{}
	return &d, nil
}

func (w *DefaultWorker) Work(id int, jobs <-chan Job, results chan<- Result) {
	fileParser := parser.NewDefaultParser()
	for work := range jobs {
		fileParser.Parse(work.Fname)
		res := Result{
			Status: true,
			Job:    work,
			Result: fmt.Sprintf("%d completed %v", id, work),
		}
		results <- res
	}
}
