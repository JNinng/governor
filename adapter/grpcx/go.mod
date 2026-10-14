module github.com/jninng/governor/adapter/grpcx

go 1.25.0

require (
	github.com/jninng/governor v0.0.0-00010101000000-000000000000
	github.com/jninng/observ v0.1.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800
	google.golang.org/grpc v1.84.0
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/jninng/governor => ../..
