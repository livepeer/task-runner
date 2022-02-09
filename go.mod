module github.com/livepeer/task-runner

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/livepeer/go-api-client v0.0.0-20220214143256-1336ffc6b497
	github.com/livepeer/go-livepeer v0.5.27-0.20220201164915-5da8ff8e521c
	github.com/livepeer/livepeer-data v0.4.12-0.20220214150817-e2fef8e66181
	github.com/livepeer/joy4 v0.1.2-0.20220210094601-95e4d28f5f07
	github.com/livepeer/livepeer-data v0.4.12-0.20220210052732-88531339755f
	github.com/livepeer/stream-tester v0.11.4-0.20220210094813-3f56876c3ac8
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
