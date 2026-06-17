module github.com/yadukrishnan2004/antflow-sdk

go 1.26.3

replace github.com/yadukrishnan2004/antflow-server => ../antflow-server

require (
	github.com/lib/pq v1.12.3
	github.com/yadukrishnan2004/antflow-server v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.81.1
)

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
