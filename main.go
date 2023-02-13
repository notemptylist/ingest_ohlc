package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path"
	"runtime"
	"sync"

	"github.com/notemptylist/ingest_ohlc/worker"
)

type dataDir struct {
	path      string
	filecount int
	sync.RWMutex
	processed map[string]string
	files     []string
}

func newDataDir(path string) *dataDir {
	return &dataDir{
		path:      path,
		files:     make([]string, 0),
		processed: make(map[string]string),
	}
}

// readFileNames Reads the file names of all files in the dataDir path
// file names which do not end in .json are ignored.
func (d *dataDir) readFilesNames() error {

	files, err := os.ReadDir(d.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if path.Ext(fname) == ".json" {
			d.files = append(d.files, fname)
		}
	}
	d.filecount = len(d.files)
	return nil
}

var jobs = make(chan worker.Job, 3)
var results = make(chan worker.Result, 3)

// allocateWork creates Job objects from the dataDir struct and sends them
// to the jobs channel
// Once all the jobs have been assigned the channel is closed.
func allocateWork(d *dataDir) {
	for id, fname := range d.files {
		d.Lock()
		_, ok := d.processed[fname]
		d.Unlock()
		if ok {
			continue
		}
		job := worker.Job{Id: id, Fname: d.path + fname}
		jobs <- job
	}
	close(jobs)
}

// spawnWorkers creates a WaitGroup, launches an appropriate number of goroutines
// and waits for their completion.
// Once the workers have exited, it's safe to close the results channel
func spawnWorkers(numWorkers int, child worker.Worker) {
	var wg sync.WaitGroup
	log.Println("Spawning workers...")
	for w := 1; w <= numWorkers; w++ {
		wId := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			child.Work(wId, jobs, results)
		}()
	}
	wg.Wait()
	// Any items in the results chan will still get processed.
	close(results)
}

func collector(d *dataDir, done chan<- bool) {
	for res := range results {
		if res.Status {
			d.Lock()
			if _, ok := d.processed[res.Job.Fname]; ok {
				log.Fatalf("Received duplicate result: %s", res.Job.Fname)
			}
			d.processed[res.Job.Fname] = res.Result
			log.Printf("[.] %s : %s", res.Job.Fname, res.Result)
			d.Unlock()
		} else {
			log.Printf("{%d} {%s} failed: {%s}\n", res.Job.Id, res.Job.Fname, res.Result)
		}
	}
	done <- true
}

func main() {

	numProcs := runtime.NumCPU()
	dataDirPath := flag.String("dataDir", "", "Path to the data directory")
	dbUrl := flag.String("dbUrl", "", "Database connection URI")
	numProcs = *flag.Int("workers", numProcs, "Number of workers to spawn")

	flag.Parse()
	if len(*dbUrl) == 0 || len(*dataDirPath) == 0 {
		flag.Usage()
		return
	}
	log.Printf("Running with %d workers\n", numProcs)
	log.Printf("Using %s as dbUrl\n", *dbUrl)

	D := newDataDir(*dataDirPath)
	if err := D.readFilesNames(); err != nil {
		log.Fatal(err)
	}
	log.Printf("Discovered %d JSON files\n", D.filecount)

	go allocateWork(D)
	done := make(chan bool)
	go collector(D, done)
	// child := worker.NewDefaultWorker()
	dbConn, err := sql.Open("postgres", *dbUrl)
	if err != nil {
		panic(err)
	}
	child, err := worker.NewPostgresWorker(dbConn)
	if err != nil {
		log.Fatal(err)
	}
	spawnWorkers(numProcs, child)
	<-done
	log.Printf("[*] Processed %d/%d files.", len(D.processed), D.filecount)
}
