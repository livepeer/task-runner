module github.com/livepeer/task-runner

go 1.16

require (
	github.com/golang/glog v1.0.0
	github.com/livepeer/go-api-client v0.0.0-20220126222908-cf886f606b02
	github.com/livepeer/go-livepeer v0.5.26
	github.com/livepeer/livepeer-data v0.4.8
	github.com/peterbourgon/ff v1.7.1
	github.com/rabbitmq/amqp091-go v1.1.0
)

replace (
	github.com/livepeer/go-api-client => ../go-api-client
	github.com/livepeer/go-livepeer => ../go-livepeer
	github.com/livepeer/livepeer-data => ../livepeer-data
)
