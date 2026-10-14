// This stub makes web/ its own (empty) Go module, so `go ... ./...` from the
// repo root skips it: npm packages such as flatted ship Go files under
// web/node_modules, which otherwise get built, vetted and tested as part of
// module piperoom.
module piperoom/web

go 1.27.0
