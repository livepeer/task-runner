module github.com/livepeer/task-runner

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/golang/mock v1.6.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/livepeer/go-api-client v0.2.8-beta
	github.com/livepeer/go-tools v0.0.0-20220805063103-76df6beb6506
	github.com/livepeer/joy4 v0.1.2-0.20220210094601-95e4d28f5f07
	github.com/livepeer/livepeer-data v0.4.20-beta
	github.com/livepeer/stream-tester v0.12.12
	github.com/peterbourgon/ff v1.7.1
	github.com/prometheus/client_golang v1.11.0
	github.com/rabbitmq/amqp091-go v1.1.0
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	gopkg.in/vansante/go-ffprobe.v2 v2.0.3
)

// replace (
//   github.com/livepeer/go-api-client => ../go-api-client
//   github.com/livepeer/go-livepeer => ../go-livepeer
//   github.com/livepeer/joy4 => ../joy4
//   github.com/livepeer/livepeer-data => ../livepeer-data
//   github.com/livepeer/stream-tester => ../stream-tester
// )
