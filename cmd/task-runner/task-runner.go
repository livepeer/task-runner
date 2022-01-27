package analyzer

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/golang/glog"
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

func parseFlags() cliFlags {
	cli := cliFlags{}
	fs := flag.NewFlagSet("task-runner", flag.ExitOnError)

	fs.StringVar(&cli.runnerOpts.AMQPUri, "amqp-uri", "amqp://guest:guest@localhost:5672/livepeer", "URI for RabbitMQ server to consume from. Specified in the AMQP protocol.")
	fs.StringVar(&cli.runnerOpts.APIExchangeName, "api-exchange-name", "lp_api_tasks", "Name of exchange where the tasks will be published to.")
	fs.StringVar(&cli.runnerOpts.QueueName, "queue-name", "lp_runner_task_queue", "Name of task queue to consume from. If it doesn't exist a new queue will be created and bound to the API exchange.")

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
	return cli
}

func Run(build BuildFlags) {
	glog.Infof("Task runner starting... version=%q", build.Version)
	ctx := contextUntilSignal(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	cli := parseFlags()
	runner := task.NewRunner(cli.runnerOpts)

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
