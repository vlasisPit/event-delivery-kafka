test_all:
	go test -v ./...

go_run:
	go run main.go

go_run_without_kafka_internal_logs:
	go run main.go | grep -v "reader"

