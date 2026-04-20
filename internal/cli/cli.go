package cli

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	mcpserver "github.com/dpopsuev/chronolog/internal/mcp"

	"github.com/dpopsuev/chronolog/internal/config"
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
	return &cobra.Command{
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

			slog.InfoContext(ctx, "starting chronolog MCP server",
				slog.String(logKeyTransport, cfg.Transport),
				slog.String(logKeyDB, cfg.DB.Path),
			)

			return srv.Serve(ctx, &sdkmcp.StdioTransport{})
		},
	}
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
