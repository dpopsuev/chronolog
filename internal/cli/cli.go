package cli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	batterymcp "github.com/dpopsuev/battery/mcp"
	"github.com/dpopsuev/chronolog/internal/config"
	mcpserver "github.com/dpopsuev/chronolog/internal/mcp"
	"github.com/dpopsuev/chronolog/internal/store"
	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

// Slog attribute key constants.
const (
	logKeyTransport = "transport"
	logKeyDB        = "db"
)

var (
	version    string
	configPath string
)

// Execute runs the root command.
func Execute(v string) error {
	version = v
	return rootCmd().Execute()
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "chronolog",
		Short: "Log consolidation MCP server",
	}
	root.PersistentFlags().StringVar(&configPath, "config", "", "config file path")

	root.AddCommand(versionCmd(), serveCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run:   func(_ *cobra.Command, _ []string) { fmt.Printf("chronolog %s\n", version) },
	}
}

func serveCmd() *cobra.Command {
	var transport, addr string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Chronolog MCP server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			initLogger()

			cfg, err := config.Resolve(configPath)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}

			s, err := store.OpenSQLite(cfg.DB.Path, cfg.DB.BusyTimeoutMs)
			if err != nil {
				return fmt.Errorf("open database: %w", err)
			}
			defer s.Close()

			srv := mcpserver.NewServer(s, version)

			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
			defer stop()

			if transport == "" {
				transport = cfg.Transport
			}

			slog.InfoContext(ctx, "starting chronolog MCP server",
				slog.String(logKeyTransport, transport),
				slog.String(logKeyDB, cfg.DB.Path),
			)

			if transport == "http" {
				return serveHTTP(ctx, srv, addr)
			}
			return srv.Serve(ctx, &sdkmcp.StdioTransport{})
		},
	}
	cmd.Flags().StringVar(&transport, "transport", "", "transport: stdio (default) or http")
	cmd.Flags().StringVar(&addr, "addr", ":8080", "HTTP listen address (only with --transport=http)")
	return cmd
}

func serveHTTP(ctx context.Context, srv *batterymcp.Server, addr string) error {
	sdkSrv := srv.SDK()
	handler := sdkmcp.NewStreamableHTTPHandler(func(_ *http.Request) *sdkmcp.Server {
		return sdkSrv
	}, &sdkmcp.StreamableHTTPOptions{
		Logger: slog.Default(),
	})

	mux := http.NewServeMux()
	mux.Handle("/mcp", handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"status":"ok"}`)
	})

	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 30 * time.Second} //nolint:mnd // standard timeout

	go func() {
		<-ctx.Done()
		httpSrv.Close()
	}()

	slog.InfoContext(ctx, "HTTP server listening", slog.String(logKeyTransport, addr))
	err := httpSrv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func initLogger() {
	level := slog.LevelInfo
	if v := os.Getenv("CHRONOLOG_LOG_LEVEL"); v != "" {
		switch strings.ToLower(v) {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
