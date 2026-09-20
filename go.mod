module github.com/Viking602/venat

go 1.25.0

toolchain go1.25.13

retract v0.10.0 // Published from a reused tag; its public checksum cannot verify.

require (
	github.com/Viking602/llmux v0.4.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/text v0.14.0 // indirect
