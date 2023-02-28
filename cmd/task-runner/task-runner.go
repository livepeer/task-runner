package analyzer

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/livepeer/stream-tester/m3u8"
	"github.com/livepeer/task-runner/api"
	"github.com/livepeer/task-runner/clients"
	"github.com/livepeer/task-runner/metrics"
	"github.com/livepeer/task-runner/task"
	"github.com/peterbourgon/ff"
	"golang.org/x/sync/errgroup"
)

// Build flags to be overwritten at build-time and passed to Run()
type BuildFlags struct {
	Version string
}

type cliFlags struct {
	catalystOpts clients.CatalystOptions
	runnerOpts   task.RunnerOptions
	serverOpts   api.ServerOptions
}

func parseURL(s string, dest **url.URL) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if _, err = url.ParseQuery(u.RawQuery); err != nil {
		return err
	}
	*dest = u
	return nil
}

func URLVarFlag(fs *flag.FlagSet, dest **url.URL, name, value, usage string) {
	if err := parseURL(value, dest); err != nil {
		panic(err)
	}
	fs.Func(name, usage, func(s string) error {
		return parseURL(s, dest)
	})
}

func parseURLs(s string, dest *[]*url.URL) error {
	strs := strings.Split(s, ",")
	urls := make([]*url.URL, len(strs))
	for i, str := range strs {
		if err := parseURL(str, &urls[i]); err != nil {
			return err
		}
	}
	*dest = urls
	return nil
}

func URLSliceVarFlag(fs *flag.FlagSet, dest *[]*url.URL, name, value, usage string) {
	if err := parseURLs(value, dest); err != nil {
		panic(err)
	}
	fs.Func(name, usage, func(s string) error {
		return parseURLs(s, dest)
	})
}

func parseFlags(build BuildFlags) cliFlags {
	cli := cliFlags{}
	fs := flag.NewFlagSet("livepeer-task-runner", flag.ExitOnError)

	// Catalyst options
	fs.StringVar(&cli.catalystOpts.BaseURL, "catalyst-url", "http://localhost:7979", "Base URL to the Catalyst API")
	URLVarFlag(fs, &cli.catalystOpts.OwnBaseURL, "own-base-url", "http://localhost:8082/task-runner", "Base URL to reach the current server. This must include the API root")
	fs.StringVar(&cli.catalystOpts.Secret, "catalyst-secret", "IAmAuthorized", "Auth secret for the Catalyst API")

	// Runner options
	fs.StringVar(&cli.runnerOpts.AMQPUri, "amqp-uri", "amqp://guest:guest@localhost:5672/livepeer", "URI for RabbitMQ server to consume from. Specified in the AMQP protocol")
	fs.StringVar(&cli.runnerOpts.ExchangeName, "exchange-name", "lp_tasks", "Name of exchange where the task events will be published to")
	fs.StringVar(&cli.runnerOpts.QueueName, "queue-name", "lp_runner_task_queue", "Name of task queue to consume from. If it doesn't exist a new queue will be created and bound to the API exchange")
	fs.StringVar(&cli.runnerOpts.OldQueueName, "old-queue-name", "", "Name of the old task queue that should be unbound and attempted a clean-up")
	fs.StringVar(&cli.runnerOpts.DeadLetter.ExchangeName, "dead-letter-exchange-name", "", "Name of the dead letter exchange to create for tasks that are unprocessable")
	fs.StringVar(&cli.runnerOpts.DeadLetter.QueueName, "dead-letter-queue-name", "", "Name of the queue where the dead-lettered tasks should be routed to. This queue is not consumed automatically. If empty, the name will be the same as the dead-letter exchange")
	fs.DurationVar(&cli.runnerOpts.MinTaskProcessingTime, "min-task-processing-time", task.DefaultMinTaskProcessingTime, "Minimum time that a task processing must take as rate-limiting strategy. If the task finishes earlier, the runner will wait for the remaining time before starting another task")
	fs.DurationVar(&cli.runnerOpts.MaxTaskProcessingTime, "max-task-processing-time", task.DefaultMaxTaskProcessingTime, "Timeout for task processing. If the task is not completed within this time it will be marked as failed")
	fs.UintVar(&cli.runnerOpts.MaxConcurrentTasks, "max-concurrent-tasks", task.DefaultMaxConcurrentTasks, "Maximum number of tasks to run simultaneously")
	fs.BoolVar(&cli.runnerOpts.HumanizeErrors, "humanize-errors", true, "Whether to process task error messages into human-friendlier ones")
	fs.StringVar(&cli.runnerOpts.LivepeerAPIOptions.Server, "livepeer-api-server", "localhost:3004", "Base URL for a custom server to use for the Livepeer API")
	fs.StringVar(&cli.runnerOpts.LivepeerAPIOptions.AccessToken, "livepeer-access-token", "", "Access token for Livepeer API")
	fs.StringVar(&cli.runnerOpts.PinataAccessToken, "pinata-access-token", "", "JWT access token for the Piñata API")
	URLVarFlag(fs, &cli.runnerOpts.PlayerImmutableURL, "player-immutable-url", "ipfs://bafybeihcqgu4rmsrlkqvavkzsnu7h5n66jopckes6u5zrhs3kcffqvylge/", "Base URL for an immutable version of the Livepeer Player to be included in NFTs metadata")
	URLVarFlag(fs, &cli.runnerOpts.PlayerExternalURL, "player-external-url", "https://lvpr.tv/", "Base URL for the updateable version of the Livepeer Player to be included in NFTs external URL")
	URLSliceVarFlag(fs, &cli.runnerOpts.ImportIPFSGatewayURLs, "import-ipfs-gateway-urls", "https://w3s.link/ipfs/,https://ipfs.io/ipfs/,https://cloudflare-ipfs.com/ipfs/", "Comma delimited ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from")

	// Server options
	fs.StringVar(&cli.serverOpts.Host, "host", "localhost", "Hostname to bind to")
	fs.UintVar(&cli.serverOpts.Port, "port", 8082, "Port to listen on")
	fs.DurationVar(&cli.serverOpts.ShutdownGracePeriod, "shutdown-grace-perod", 30*time.Second, "Grace period to wait for shutdown before using the force")
	// API Handler
	fs.StringVar(&cli.serverOpts.APIRoot, "api-root", "/task-runner", "Root path where to bind the API to")
	fs.BoolVar(&cli.serverOpts.Prometheus, "prometheus", false, "Whether to enable Prometheus metrics registry and expose /metrics endpoint")

	mistJson := fs.Bool("j", false, "Print application info as json")

	flag.Set("logtostderr", "true")
	glogVFlag := flag.Lookup("v")
	verbosity := fs.Int("v", 0, "Log verbosity {0-10}")

	fs.String("config", "", "config file (optional)")
	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LP"),
		ff.WithEnvVarIgnoreCommas(true),
	)
	flag.CommandLine.Parse(nil)
	glogVFlag.Value.Set(strconv.Itoa(*verbosity))

	if *mistJson {
		mistconnector.PrintMistConfigJson(
			"livepeer-task-runner",
			"Livepeer task processing application. Does imports/exports to S3 and IPFS and such.",
			"Livepeer Task Runner",
			build.Version,
			fs,
		)
		os.Exit(0)
	}
	return cli
}

func Run(build BuildFlags) {
	cli := parseFlags(build)
	glog.Infof("Task runner starting... version=%q", build.Version)

	// Fire a metric to track which version of task-runner we're running
	metrics.Version.WithLabelValues("task-runner", build.Version).Inc()

	clients.UserAgent = "livepeer-task-runner/" + build.Version
	cli.runnerOpts.LivepeerAPIOptions.UserAgent = clients.UserAgent
	cli.serverOpts.APIHandlerOptions.ServerName = clients.UserAgent
	cli.runnerOpts.Catalyst, cli.serverOpts.Catalyst = &cli.catalystOpts, &cli.catalystOpts
	m3u8.InitCensus("task-runner", build.Version)

	runner := task.NewRunner(cli.runnerOpts)
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := runner.Start(); err != nil {
			return fmt.Errorf("error starting runner: %w", err)
		}
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), cli.serverOpts.ShutdownGracePeriod)
		defer cancel()
		glog.Infof("Task runner shutting down...")
		if err := runner.Shutdown(shutCtx); err != nil {
			return fmt.Errorf("runner shutdown error: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		glog.Info("Starting API server...")
		err := api.ListenAndServe(ctx, runner, cli.serverOpts)
		if err != nil {
			return fmt.Errorf("api server error: %w", err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		glog.Fatalf("Fatal error on task-runner: %v", err)
	}
}

func contextUntilSignal(parent context.Context, sigs ...os.Signal) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		defer cancel()
		waitSignal(sigs...)
	}()
	return ctx
}

func waitSignal(sigs ...os.Signal) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, sigs...)
	defer signal.Stop(sigc)

	signal := <-sigc
	switch signal {
	case syscall.SIGINT:
		glog.Infof("Got Ctrl-C, shutting down")
	case syscall.SIGTERM:
		glog.Infof("Got SIGTERM, shutting down")
	default:
		glog.Infof("Got signal %d, shutting down", signal)
	}
}
