package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v4"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/stat"
)

type change struct {
	at                        time.Time
	writer                    int
	index                     int
	updateBefore, updateAfter time.Time
	values                    [][]byte
	duplicate                 int
}

type args struct {
	For      time.Duration
	QPS      float64
	Quiet    bool
	Watchers int
	Writers  int
}

func main() {
	var f args
	flag.Float64Var(&f.QPS, "qps", 10, "Updates per second (QPS) to execute (-1 for as fast as possible)")
	flag.DurationVar(&f.For, "for", 15*time.Second, "How long to execute the benchmark")
	flag.BoolVar(&f.Quiet, "q", false, "Suppress incremental log output if true")
	flag.IntVar(&f.Watchers, "extra-watchers", 0, "Number of additional watchers to run (one watcher per writer always runs)")
	flag.IntVar(&f.Writers, "writers", 1, "Number of writers")
	flag.Parse()

	if f.QPS <= 0 && f.QPS != -1 {
		log.Fatal("-qps must be positive or -1")
	}
	if f.For <= 0 {
		log.Fatal("-for must be positive")
	}
	if f.Watchers < 0 {
		log.Fatal("-watchers must be non-negative")
	}
	if f.Writers <= 0 {
		log.Fatal("-writers must be positive")
	}

	// Attempt to connect
	config, err := pgx.ParseConfig("postgresql://root@ccdev9:26257/defaultdb?sslmode=disable")
	if err != nil {
		log.Fatal("error configuring the database: ", err)
	}

	ctx := context.Background()

	var estimatedWrites int
	var resultBuffer int
	var writeInterval time.Duration
	if f.QPS > 0 {
		writes := int(math.Ceil(f.For.Seconds() * f.QPS))
		estimatedWrites = writes
		resultBuffer = writes * 2 * f.Writers
		writeInterval = time.Duration(time.Second.Seconds() / f.QPS * float64(time.Second))
	} else {
		resultBuffer = 100000
		estimatedWrites = 10000
	}

	var upgradeGroup errgroup.Group
	var startGroup sync.WaitGroup
	startGroup.Add(f.Writers)
	var updateStart time.Time
	updateRun := make(chan struct{})

	writerUpdates := make([][]change, f.Writers)
	for k := range writerUpdates {
		writerUpdates[k] = make([]change, 0, estimatedWrites)
	}

	timeoutInterval := 10 * time.Second
	for writer := 0; writer < f.Writers; writer++ {
		k := writer
		upgradeGroup.Go(func() error {
			connWrite, err := pgx.ConnectConfig(context.Background(), config)
			if err != nil {
				startGroup.Done()
				return fmt.Errorf("error connecting to the database: %v", err)
			}
			defer connWrite.Close(context.Background())

			if _, err := connWrite.Exec(ctx, `INSERT INTO foo (a, i) VALUES ($1, -1) ON CONFLICT (a) DO UPDATE SET i = -1;`, k); err != nil {
				startGroup.Done()
				return fmt.Errorf("unable to reset state: %v", err)
			}

			sd, err := connWrite.Prepare(ctx, "increment", "UPDATE foo SET i=$2 where a=$1")
			if err != nil {
				startGroup.Done()
				return fmt.Errorf("preparing update failed: %v", err)
			}

			startGroup.Done()
			startGroup.Wait()
			<-updateRun

			for i := 0; ; i++ {
				tUpdateBefore := time.Now()
				if _, err := connWrite.Exec(ctx, sd.Name, k, i); err != nil {
					return fmt.Errorf("failed to write: %v", err)
				}
				tUpdateAfter := time.Now()
				d := tUpdateAfter.Sub(tUpdateBefore)
				if !f.Quiet {
					fmt.Printf("%s update[%d,%d]: %s\n", time.Now().Format(time.RFC3339Nano), k, i, d.Round(time.Microsecond))
				}

				writerUpdates[k] = append(writerUpdates[k], change{
					writer:       k,
					index:        i,
					updateBefore: tUpdateBefore,
					updateAfter:  tUpdateAfter,
				})

				if f.QPS > 0 {
					if i > estimatedWrites {
						break
					}
				} else {
					if tUpdateAfter.After(updateStart.Add(f.For)) {
						//log.Printf("done writing")
						break
					}
				}
				if d < writeInterval {
					time.Sleep(writeInterval - d)
				}
			}
			return nil
		})
	}

	// wait for all clients to open their connections, then set updateStart, then allow the
	// workers to run
	startGroup.Wait()

	// start the watchers now that all writers are ready to go
	var watcherGroup errgroup.Group
	ctxChangefeed, cancelChangeFeedFn := context.WithCancel(ctx)
	results := make(chan change, resultBuffer)
	for watcher := 0; watcher < (1 + f.Watchers); watcher++ {
		i := watcher
		watcherGroup.Go(func() error {
			connRead, err := pgx.ConnectConfig(context.Background(), config)
			if err != nil {
				return fmt.Errorf("error connecting to the database: ", err)
			}
			defer connRead.Close(context.Background())
			rows, err := connRead.Query(ctxChangefeed, "experimental changefeed for foo with updated,mvcc_timestamp,diff,no_initial_scan")
			if err != nil {
				return fmt.Errorf("connection failure: %v", err)
			}
			for rows.Next() {
				v := rows.RawValues()
				if len(v) != 3 {
					log.Fatalf("unexpected number of rows: %d", len(v))
				}

				// if we are not the first changefeed, simply read the values
				if i != 0 {
					continue
				}

				at := time.Now()
				slice := make([][]byte, len(v))
				copy(slice, v)
				results <- change{at: at, values: slice}
				if !f.Quiet {
					fmt.Printf("%s row: %s, %s, %s\n", time.Now().Format(time.RFC3339Nano), string(v[0]), string(v[1]), string(v[2]))
				}
			}
			if err := rows.Err(); err != nil {
				if !strings.Contains(err.Error(), "context canceled") {
					log.Printf("failed to query rows: %v", err)
					return fmt.Errorf("failed to query rows: %v", err)
				}
			}
			return nil
		})
	}

	// drain the result channels for any writes
	wait := 1 * time.Second
	for clear := false; !clear; {
		select {
		case r := <-results:
			log.Printf("Unexpected write in result channel: %v", r)
		case <-time.After(wait):
			clear = true
		}
	}

	if f.QPS >= 0 {
		fmt.Printf("Run: iterations=%d writers=%d watchers=%d qps=%s duration=%s ...\n", estimatedWrites, f.Writers, f.Writers+f.Watchers, strconv.FormatFloat(f.QPS, 'f', -1, 64), f.For)
	} else {
		fmt.Printf("Run: duration=%s writers=%d watchers=%d ...\n", f.For, f.Writers, f.Writers+f.Watchers)
	}

	// run the test
	updateStart = time.Now()
	close(updateRun)

	if err := upgradeGroup.Wait(); err != nil {
		log.Fatalf("failed to run updates: %v", err)
	}
	updateEnd := time.Now()

	var totalWrites int
	for _, updates := range writerUpdates {
		totalWrites += len(updates)
	}

	received, duplicateEvents, outOfRangeEvents := 0, 0, 0
	m := make(map[string]interface{})
	for received < totalWrites {
		select {
		case r := <-results:
			for k := range m {
				delete(m, k)
			}
			if err := json.Unmarshal(r.values[2], &m); err != nil {
				log.Fatalf("unable to unmarshal result: %v", err)
			}
			after, ok := m["after"].(map[string]interface{})
			if !ok {
				log.Printf("no 'after' object: %v", m)
				continue
			}
			f, ok := after["i"].(float64)
			if !ok {
				log.Printf("no after.i int: %v", m)
				continue
			}
			i := int(f)
			f, ok = after["a"].(float64)
			if !ok {
				log.Printf("no after.a int: %v", m)
				continue
			}
			k := int(f)

			if k > len(writerUpdates) || i >= len(writerUpdates[k]) {
				outOfRangeEvents++
				continue
			}
			updates := writerUpdates[k]
			if updates[i].values != nil {
				updates[i].duplicate++
				duplicateEvents++
				//log.Printf("received duplicate event for %d", i)
				continue
			}
			received++

			updates[i].values = r.values
			updates[i].at = r.at
			r = updates[i]

			//latencyMax := r.at.Sub(r.updateBefore).Round(time.Microsecond)
			//latencyMin := r.at.Sub(r.updateAfter).Round(time.Microsecond)
			//fmt.Printf("%s result[%d] with latency %s - %s\n", time.Now().Format(time.RFC3339Nano), i, latencyMin, latencyMax)
		case <-time.After(timeoutInterval):
			log.Fatalf("timeout waiting for a result")
		}
	}

	series := map[string][]float64{}
	for _, updates := range writerUpdates {
		totalWrites += len(updates)
		for _, r := range updates {
			series["update"] = append(series["update"], r.updateAfter.Sub(r.updateBefore).Seconds())
			series["before_update -> receive"] = append(series["before_update -> receive"], r.at.Sub(r.updateBefore).Seconds())
			series["after_update -> receive"] = append(series["after_update -> receive"], r.at.Sub(r.updateAfter).Seconds())
		}
	}

	var keys []string
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 0, 2, 1, ' ', 0)
	fmt.Fprintf(w, "LABEL\tSTDDEV\tMIN\tP50\tP90\tP99\tMAX\n")
	for _, k := range keys {
		stats(w, k, series[k])
	}
	w.Flush()
	fmt.Printf(buf.String())

	updateDuration := updateEnd.Sub(updateStart).Truncate(time.Millisecond)
	fmt.Printf("Result: writes=%d duration=%s qps=%.2f duplicates=%d out-of-range=%d\n", totalWrites, updateDuration, float64(totalWrites)/updateDuration.Seconds(), duplicateEvents, outOfRangeEvents)

	cancelChangeFeedFn()
	if err := watcherGroup.Wait(); err != nil {
		log.Fatalf("Error shutting down watchers: %v", err)
	}
}

func stats(w io.Writer, label string, values []float64) {
	sort.Float64s(values)
	fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		label,
		time.Duration(stat.StdDev(values, nil)*float64(time.Second)).Round(time.Microsecond),
		time.Duration(statMin(values)*float64(time.Second)).Round(time.Microsecond),
		time.Duration(stat.Quantile(0.5, stat.LinInterp, values, nil)*float64(time.Second)).Round(time.Microsecond),
		time.Duration(stat.Quantile(0.9, stat.LinInterp, values, nil)*float64(time.Second)).Round(time.Microsecond),
		time.Duration(stat.Quantile(0.99, stat.LinInterp, values, nil)*float64(time.Second)).Round(time.Microsecond),
		time.Duration(statMax(values)*float64(time.Second)).Round(time.Microsecond),
	)
}

func statMax(x []float64) float64 {
	max := -math.MaxFloat64
	for _, f := range x {
		if f > max {
			max = f
		}
	}
	return max
}
func statMin(x []float64) float64 {
	max := math.MaxFloat64
	for _, f := range x {
		if f < max {
			max = f
		}
	}
	return max
}
