module github.com/livepeer/task-runner

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/livepeer/go-api-client v0.0.0-20220204011721-0c80b6d557db
	github.com/livepeer/go-livepeer v0.5.27-0.20220201164915-5da8ff8e521c
	github.com/livepeer/livepeer-data v0.4.9-0.20220203234012-1c5607b4633c
	github.com/peterbourgon/ff v1.7.1
	github.com/rabbitmq/amqp091-go v1.1.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	gopkg.in/vansante/go-ffprobe.v2 v2.0.3
)

replace (
	github.com/livepeer/go-api-client => ../go-api-client
	github.com/livepeer/go-livepeer => ../go-livepeer
	github.com/livepeer/livepeer-data => ../livepeer-data
)
