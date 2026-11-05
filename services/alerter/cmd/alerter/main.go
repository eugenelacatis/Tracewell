package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eugenelacatis/tracewell/services/alerter/internal/consumer"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/delivery"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/engine"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/server"
	"github.com/eugenelacatis/tracewell/services/alerter/internal/store"
)

func main() {
	// JSON structured logging
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Config from env vars only
	httpAddr := getEnv("ALERTER_HTTP_ADDR", ":8083")
	kafkaBrokersRaw := getEnv("ALERTER_KAFKA_BROKERS", "kafka:29092")
	groupID := getEnv("ALERTER_KAFKA_GROUP_ID", "alerter")
	topic := getEnv("ALERTER_TOPIC", "traces.raw")
	postgresDSN := getEnv("ALERTER_POSTGRES_DSN", "postgres://tracewell:tracewell@postgres:5432/tracewell?sslmode=disable")
	reloadInterval := 30 * time.Second

	logger.Info("starting alerter service",
		"http_addr", httpAddr,
		"kafka_brokers", kafkaBrokersRaw,
		"group_id", groupID,
		"topic", topic,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Connect to Postgres
	pgStore, err := store.NewPgStore(ctx, postgresDSN)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgStore.Close()

	// 2. Initialize Dispatcher & Engine
	dispatcher := delivery.NewChannelDispatcher(logger, nil)
	alertEngine := engine.NewAlertEngine(pgStore, dispatcher, logger, time.Now)

	// 3. Initial rule load
	if err := alertEngine.ReloadRules(ctx); err != nil {
		logger.Warn("initial rule load failed, will retry periodically", "error", err)
	}

	// 4. Start periodic rule reload ticker (every 30s)
	go func() {
		ticker := time.NewTicker(reloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reloadCtx, reloadCancel := context.WithTimeout(ctx, 10*time.Second)
				if err := alertEngine.ReloadRules(reloadCtx); err != nil {
					logger.Error("scheduled rule reload failed", "error", err)
				}
				reloadCancel()
			}
		}
	}()

	// 5. Ready check for /readyz (Postgres ping + Kafka connection check)
	brokers := consumer.SplitBrokers(kafkaBrokersRaw)
	readyCheck := func(checkCtx context.Context) error {
		if err := pgStore.Ping(checkCtx); err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		// Check at least one Kafka broker is reachable
		var dialErr error
		for _, b := range brokers {
			d := net.Dialer{Timeout: 1 * time.Second}
			conn, err := d.DialContext(checkCtx, "tcp", b)
			if err == nil {
				_ = conn.Close()
				return nil
			}
			dialErr = err
		}
		return fmt.Errorf("kafka brokers unreachable: %w", dialErr)
	}

	// 6. Start HTTP server (:8083)
	httpSrv := server.NewServer(httpAddr, readyCheck)
	go func() {
		if err := httpSrv.Start(); err != nil {
			logger.Error("HTTP server error", "error", err)
		}
	}()

	// 7. Start Kafka Consumer
	kafkaConsumer := consumer.NewConsumer(brokers, groupID, topic, alertEngine, logger)
	go func() {
		if err := kafkaConsumer.Run(ctx); err != nil {
			logger.Error("kafka consumer error", "error", err)
		}
	}()

	// 8. Graceful shutdown on SIGTERM / SIGINT
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigChan
	logger.Info("received termination signal, starting graceful shutdown", "signal", sig.String())

	// Stop accepting new messages and cancel background work
	cancel()
	_ = kafkaConsumer.Close()

	// Shutdown HTTP server with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("error during HTTP server shutdown", "error", err)
	}

	logger.Info("alerter service stopped cleanly")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
