module github.com/livepeer/task-runner

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/livepeer/go-api-client v0.1.5
	github.com/livepeer/go-livepeer v0.5.31
	github.com/livepeer/joy4 v0.1.2-0.20220210094601-95e4d28f5f07
	github.com/livepeer/livepeer-data v0.4.14
	github.com/livepeer/stream-tester v0.12.10
	github.com/peterbourgon/ff v1.7.1
	github.com/rabbitmq/amqp091-go v1.1.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	gopkg.in/vansante/go-ffprobe.v2 v2.0.3
)

// replace (
// 	github.com/livepeer/go-api-client => ../go-api-client
// 	github.com/livepeer/go-livepeer => ../go-livepeer
// 	github.com/livepeer/joy4 => ../joy4
// 	github.com/livepeer/livepeer-data => ../livepeer-data
// 	github.com/livepeer/stream-tester => ../stream-tester
// )
