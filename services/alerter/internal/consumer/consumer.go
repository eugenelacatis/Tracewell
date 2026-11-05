package consumer

import (
	"context"
	"log/slog"
	"strings"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/engine"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// Consumer consumes IngestBatches from Kafka traces.raw and passes them to engine.
type Consumer struct {
	reader *kafka.Reader
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

	return &Consumer{
		reader: reader,
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

		var batch tracewellv1.IngestBatch
		if err := proto.Unmarshal(msg.Value, &batch); err != nil {
			c.logger.Warn("failed to unmarshal IngestBatch protobuf",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error", err,
			)
			// Commit bad message so consumer group doesn't block
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.engine.ProcessBatch(ctx, &batch); err != nil {
			c.logger.Error("failed to process batch in alert engine",
				"project_id", batch.ProjectId,
				"error", err,
			)
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
	return c.reader.Close()
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
