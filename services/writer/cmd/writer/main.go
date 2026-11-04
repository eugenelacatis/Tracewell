// writer-svc consumes traces.raw and evals.results (consumer group "writer") and stores them in
// ClickHouse. Offsets are committed only after the rows are inserted or the message is in
// traces.dlq, so a crash at any point means redelivery, never loss. Redelivery is safe because
// every table is a ReplacingMergeTree.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/writer/internal/pipeline"
	"github.com/eugenelacatis/tracewell/services/writer/internal/store"
)

const (
	group         = "writer"
	topicDLQ      = "traces.dlq"
	flushRows     = 500
	flushInterval = time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("writer-svc exited", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// kafkaDLQ produces DeadLetters with the original message's key and waits for the ack.
type kafkaDLQ struct{ client *kgo.Client }

func (d kafkaDLQ) Send(ctx context.Context, key []byte, dl *tracewellv1.DeadLetter) error {
	value, err := proto.Marshal(dl)
	if err != nil {
		return err
	}
	return d.client.ProduceSync(ctx, &kgo.Record{Topic: topicDLQ, Key: key, Value: value}).FirstErr()
}

func run(logger *slog.Logger) error {
	addr := env("WRITER_HTTP_ADDR", ":8084")
	brokers := strings.Split(env("WRITER_KAFKA_BROKERS", "localhost:9092"), ",")
	chDSN := env("WRITER_CLICKHOUSE_DSN", "clickhouse://tracewell:tracewell@localhost:9000/tracewell")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	ch, err := store.OpenClickHouse(chDSN)
	if err != nil {
		return err
	}
	defer ch.Close()

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(pipeline.TopicTraces, pipeline.TopicEvals),
		// We commit by hand, after the insert. Auto-commit would commit on a timer, possibly
		// before the rows are in ClickHouse, and a crash then would lose them.
		kgo.DisableAutoCommit(),
		// Hold off partition reassignment while we have uncommitted records buffered, so we never
		// commit offsets for a partition another writer instance has already taken over.
		kgo.BlockRebalanceOnPoll(),
		// A brand-new group starts from the oldest retained message rather than only new ones.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RequiredAcks(kgo.AllISRAcks()), // for the DLQ producer
	)
	if err != nil {
		return err
	}

	health := http.NewServeMux()
	health.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") })
	health.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		for name, ping := range map[string]func(context.Context) error{"kafka": kafka.Ping, "clickhouse": ch.Ping} {
			if err := ping(pctx); err != nil {
				http.Error(w, name+" unreachable", http.StatusServiceUnavailable)
				return
			}
		}
		io.WriteString(w, "ok")
	})
	health.Handle("GET /metrics", promhttp.Handler())
	httpSrv := &http.Server{Addr: addr, Handler: health, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server failed", "err", err)
			stop()
		}
	}()
	defer httpSrv.Close()

	w := &pipeline.Writer{
		Inserter:    ch,
		DeadLetters: kafkaDLQ{kafka},
		Logger:      logger,
		Backoff:     pipeline.DefaultBackoff,
	}
	logger.Info("writer-svc consuming", "addr", addr, "kafka_brokers", brokers, "group", group)
	err = consume(ctx, kafka, w, logger)
	// Leave the group cleanly so the partitions are reassigned right away instead of after the
	// session timeout. Runs after the final flush and commit inside consume.
	kafka.Close()
	return err
}

// consume polls, buffers, and flushes every flushRows rows or flushInterval, whichever comes first.
// On SIGTERM it flushes and commits what's buffered, then returns.
func consume(ctx context.Context, kafka *kgo.Client, w *pipeline.Writer, logger *slog.Logger) error {
	var buffered []*kgo.Record
	var deadline time.Time

	flush := func() error {
		// A fresh context: on shutdown ctx is already cancelled, but this last flush must still run.
		fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := w.Flush(fctx); err != nil {
			return err // not committed, so Kafka redelivers these after the restart
		}
		if err := kafka.CommitRecords(fctx, buffered...); err != nil {
			// Rows are stored; the worst case is a redelivery that dedupes. Not fatal.
			logger.Warn("offset commit failed", "err", err)
		}
		buffered = buffered[:0]
		return nil
	}

	for {
		pollCtx, cancel := ctx, context.CancelFunc(func() {})
		if len(buffered) > 0 {
			pollCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		fetches := kafka.PollRecords(pollCtx, flushRows)
		cancel()

		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				logger.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if len(buffered) == 0 {
				deadline = time.Now().Add(flushInterval)
			}
			buffered = append(buffered, r)
			w.Add(pipeline.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: r.Key, Value: r.Value})
		})

		done := ctx.Err() != nil
		if len(buffered) > 0 && (done || w.Rows() >= flushRows || !time.Now().Before(deadline)) {
			if err := flush(); err != nil {
				return err
			}
		}
		if len(buffered) == 0 {
			kafka.AllowRebalance()
		}
		if done {
			logger.Info("shut down cleanly")
			return nil
		}
	}
}
