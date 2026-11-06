package consumer

import (
	"context"
	"log/slog"
	"strings"
	"time"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/engine"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// Consumer consumes IngestBatches from Kafka traces.raw and passes them to engine.
type Consumer struct {
	reader *kafka.Reader
	dlq    *kafka.Writer
	engine *engine.AlertEngine
	logger *slog.Logger
}

// NewConsumer creates a Kafka consumer configured with group ID 'alerter'.
func NewConsumer(
	brokers []string,
	groupID string,
	topic string,
	eng *engine.AlertEngine,
	logger *slog.Logger,
) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0, // Manual commit per message
	})
	dlq := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.Hash{},
		Async:    false,
	}

	return &Consumer{
		reader: reader,
		dlq:    dlq,
		engine: eng,
		logger: logger,
	}
}

// Run begins the consumption loop until ctx is canceled.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("alerter Kafka consumer running",
		"topic", c.reader.Config().Topic,
		"group_id", c.reader.Config().GroupID,
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context canceled during graceful shutdown
				return nil
			}
			c.logger.Error("error fetching kafka message", "error", err)
			continue
		}

		var processErr error
		var batch tracewellv1.IngestBatch
		for attempt := uint32(1); attempt <= 3; attempt++ {
			if err := proto.Unmarshal(msg.Value, &batch); err != nil {
				processErr = err
			} else {
				processErr = c.engine.ProcessBatch(ctx, &batch)
			}
			if processErr == nil {
				break
			}
			c.logger.Warn("failed processing Kafka message",
				"topic", msg.Topic, "partition", msg.Partition,
				"offset", msg.Offset, "attempt", attempt, "error", processErr)
		}

		if processErr != nil {
			dl := &tracewellv1.DeadLetter{
				SourceTopic: msg.Topic,
				Partition:   int32(msg.Partition),
				Offset:      msg.Offset,
				Payload:     msg.Value,
				Error:       processErr.Error(),
				Attempts:    3,
			}
			payload, err := proto.Marshal(dl)
			if err != nil {
				return err
			}
			dlqMsg := kafka.Message{Topic: "traces.dlq", Key: msg.Key, Value: payload}
			for {
				if err := c.dlq.WriteMessages(ctx, dlqMsg); err == nil {
					break
				} else {
					c.logger.Error("failed writing dead letter; source offset remains uncommitted", "error", err)
				}
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("failed to commit message offset",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
		}
	}
}

// Close shuts down the underlying Kafka reader.
func (c *Consumer) Close() error {
	err := c.reader.Close()
	if closeErr := c.dlq.Close(); err == nil {
		err = closeErr
	}
	return err
}

// SplitBrokers splits comma-delimited broker addresses.
func SplitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	var cleaned []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}
