// The admission policy's CEL checker (deploy/helm/piperoom/ci/setup): its own
// module, so cel-go is no dependency of piperoom.
module piperoom/deploy/helm/piperoom/ci/setup/celcheck

go 1.27.3

require (
	github.com/google/cel-go v0.20.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20230515195305-f3d0a9c9a5cc // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230803162519-f966b187b2e5 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230803162519-f966b187b2e5 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
