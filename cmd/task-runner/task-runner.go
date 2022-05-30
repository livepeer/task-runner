package analyzer

import (
	"context"
	"flag"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/mistconnector"
	"github.com/livepeer/task-runner/clients"
	"github.com/livepeer/task-runner/task"
	"github.com/peterbourgon/ff"
)

// Build flags to be overwritten at build-time and passed to Run()
type BuildFlags struct {
	Version string
}

type cliFlags struct {
	runnerOpts task.RunnerOptions
}

func URLVarFlag(fs *flag.FlagSet, dest **url.URL, name, value, usage string) {
	defaultUrl, err := url.Parse(value)
	if err != nil {
		panic(err)
	}
	*dest = defaultUrl

	fs.Func(name, usage, func(s string) error {
		u, err := url.Parse(s)
		if err != nil {
			return err
		}
		_, err = url.ParseQuery(u.RawQuery)
		if err != nil {
			return err
		}
		*dest = u
		return nil
	})
}

func parseFlags(build BuildFlags) cliFlags {
	cli := cliFlags{}
	fs := flag.NewFlagSet("livepeer-task-runner", flag.ExitOnError)

	fs.StringVar(&cli.runnerOpts.AMQPUri, "amqp-uri", "amqp://guest:guest@localhost:5672/livepeer", "URI for RabbitMQ server to consume from. Specified in the AMQP protocol")
	fs.StringVar(&cli.runnerOpts.ExchangeName, "exchange-name", "lp_tasks", "Name of exchange where the task events will be published to")
	fs.StringVar(&cli.runnerOpts.QueueName, "queue-name", "lp_runner_task_queue", "Name of task queue to consume from. If it doesn't exist a new queue will be created and bound to the API exchange")
	fs.StringVar(&cli.runnerOpts.LivepeerAPIOptions.Server, "livepeer-api-server", "localhost:3004", "Base URL for a custom server to use for the Livepeer API")
	fs.StringVar(&cli.runnerOpts.LivepeerAPIOptions.AccessToken, "livepeer-access-token", "", "Access token for Livepeer API")
	fs.StringVar(&cli.runnerOpts.PinataAccessToken, "pinata-access-token", "", "JWT access token for the Piñata API")
	URLVarFlag(fs, &cli.runnerOpts.PlayerImmutableURL, "player-immutable-url", "ipfs://bafybeihza7ss7ebsxqud4kcqjizygkv2uyfixfqlwl6cboavj2anvlfn3i/", "Base URL for an immutable version of the Livepeer Player to be included in NFTs metadata")
	URLVarFlag(fs, &cli.runnerOpts.PlayerExternalURL, "player-external-url", "https://lvpr.tv/", "Base URL for the updateable version of the Livepeer Player to be included in NFTs external URL")

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

	clients.UserAgent = "livepeer-task-runner/" + build.Version
	cli.runnerOpts.LivepeerAPIOptions.UserAgent = clients.UserAgent
	runner := task.NewRunner(cli.runnerOpts)

	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := runner.Start()
	if err != nil {
		glog.Fatalf("Failed to start runner: %v", err)
	}

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	glog.Infof("Task runner shutting down...")
	if err := runner.Shutdown(shutCtx); err != nil {
		glog.Fatalf("Runner shutdown error: %v", err)
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
