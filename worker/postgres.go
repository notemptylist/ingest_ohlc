package worker

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/notemptylist/ingest_ohlc/parser"
)

type PostgresWorker struct {
	conn *sql.DB
}

func NewPostgresWorker(dbConn *sql.DB) (*PostgresWorker, error) {
	if err := dbConn.Ping(); err != nil {
		return nil, err
	}
	pg := PostgresWorker{
		conn: dbConn,
	}
	return &pg, nil
}

func (pg *PostgresWorker) Work(id int, jobs <-chan Job, results chan<- Result) {
	fileParser := parser.NewDefaultParser()

	for work := range jobs {
		res := Result{
			Job: work,
		}
		if err := fileParser.Parse(work.Fname); err != nil {
			res.Status = false
			res.Result = err.Error()
			results <- res
			continue
		}
		var count int
		var errcount int
		for _, candle := range fileParser.Contents.Candles {
			insertStmt := `insert into ohlc (symbol, time, open, high, low, close, volume) values ($1, to_timestamp($2), $3, $4, $5, $6, $7)`
			_, err := pg.conn.Exec(insertStmt,
				fileParser.Contents.Symbol,
				candle.Timestamp/1000,
				candle.Open,
				candle.High,
				candle.Low,
				candle.Close,
				candle.Volume)
			if err != nil {
				errcount++
				res.Status = false
				res.Result = err.Error()
				break
			}
			count++
			res.Status = true
		}
		if res.Status {
			res.Result = fmt.Sprintf("Inserted %d rows", count)
		}

		results <- res
	}
}
