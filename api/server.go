package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sync/errgroup"
)

type ServerOptions struct {
	Host                string
	Port                uint
	ShutdownGracePeriod time.Duration
	APIHandlerOptions
}

func ListenAndServe(ctx context.Context, opts ServerOptions) error {
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		Handler: NewHandler(ctx, opts.APIHandlerOptions),
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		ln, err := net.Listen("tcp", srv.Addr)
		if err != nil {
			return fmt.Errorf("api server listen error: %w", err)
		}
		defer ln.Close()
		glog.Infof("Listening on %s", ln.Addr())

		if err := srv.Serve(ln); err != http.ErrServerClosed {
			return fmt.Errorf("api serve error: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownGracePeriod)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			if closeErr := srv.Close(); closeErr != nil {
				err = fmt.Errorf("shutdownErr=%w closeErr=%q", err, closeErr)
			}
			return fmt.Errorf("api server shutdown error: %w", err)
		}
		return nil
	})
	return eg.Wait()
}
