package server

import (
	"context"
	"errors"
	"event-delivery-kafka/kafka/components"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type KafkaWriterSuccessMock struct {}

func (mock *KafkaWriterSuccessMock) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (mock *KafkaWriterSuccessMock) Close() error {
	return nil
}

//curl -X PUT -H "Content-Type: application/json" -d '{"user_id": "user_test_1", "payload": "event click !!!!"}' localhost:8080/events
func TestReceiveEventAndProduceSuccessfully(t *testing.T) {
	producerMock := &components.Producer{Writer: &KafkaWriterSuccessMock{}}
	mux := initializeHandlers(producerMock)

	body := "{\"user_id\": \"user_test_1\", \"payload\": \"event click !!!!\"}"
	addReq, _ := http.NewRequest("PUT", "/events", strings.NewReader(body))
	addReq.Header.Add("Content-Type", "application/json")
	addReqRecorder := newRequestRecorder(addReq, mux)
	assert.Equal(t, http.StatusOK, addReqRecorder.Code)
	assert.Equal(t, "Message received and stored successfully", addReqRecorder.Body.String())
}

type KafkaWriterFailureMock struct {}

func (mock *KafkaWriterFailureMock) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return errors.New("kafka.(*Writer): Topic must not be specified for both Writer and Message")
}

func (mock *KafkaWriterFailureMock) Close() error {
	return nil
}

func TestReceiveEventAndProduceFail(t *testing.T) {
	producerMock := &components.Producer{Writer: &KafkaWriterFailureMock{}}
	mux := initializeHandlers(producerMock)

	body := "{\"user_id\": \"user_test_1\", \"payload\": \"event click !!!!\"}"
	addReq, _ := http.NewRequest("PUT", "/events", strings.NewReader(body))
	addReq.Header.Add("Content-Type", "application/json")
	addReqRecorder := newRequestRecorder(addReq, mux)
	assert.Equal(t, http.StatusInternalServerError, addReqRecorder.Code)
	assert.Equal(t, "kafka.(*Writer): Topic must not be specified for both Writer and Message", addReqRecorder.Body.String())
}

//curl -X PUT -H "Content-Type: application/xml" -d '{"user_id": "user_test_1", "payload": "event click !!!!"}' localhost:8080/events
func TestReceiveEventWithWrongContentType(t *testing.T) {
	producerMock := &components.Producer{Writer: &KafkaWriterFailureMock{}}
	mux := initializeHandlers(producerMock)

	body := "{\"user_id\": \"user_test_1\", \"payload\": \"event click !!!!\"}"
	addReq, _ := http.NewRequest("PUT", "/events", strings.NewReader(body))
	addReq.Header.Add("Content-Type", "application/xml")
	addReqRecorder := newRequestRecorder(addReq, mux)
	assert.Equal(t, http.StatusUnsupportedMediaType, addReqRecorder.Code)
	assert.Equal(t, "need content-type 'application/json', but got 'application/xml'", addReqRecorder.Body.String())
}

//curl -X PUT -H "Content-Type: application/json" -d '{"book_id": "user_test_1", "payload": "Book 123"}' localhost:8080/events
func TestReceiveEventWithoutUserId(t *testing.T) {
	producerMock := &components.Producer{Writer: &KafkaWriterFailureMock{}}
	mux := initializeHandlers(producerMock)

	body := "{\"book_id\": \"user_test_1\", \"payload\": \"Book 123\"}"
	addReq, _ := http.NewRequest("PUT", "/events", strings.NewReader(body))
	addReq.Header.Add("Content-Type", "application/json")
	addReqRecorder := newRequestRecorder(addReq, mux)
	assert.Equal(t, http.StatusBadRequest, addReqRecorder.Code)
	assert.Equal(t, "UserID should be provided", addReqRecorder.Body.String())
}

//curl -X PUT -H "Content-Type: application/json" -d '{"user_id": "", "payload": "Book 123"}' localhost:8080/events
func TestReceiveEventWithEmptyUserId(t *testing.T) {
	producerMock := &components.Producer{Writer: &KafkaWriterFailureMock{}}
	mux := initializeHandlers(producerMock)

	body := "{\"user_id\": \"\", \"payload\": \"Book 123\"}"
	addReq, _ := http.NewRequest("PUT", "/events", strings.NewReader(body))
	addReq.Header.Add("Content-Type", "application/json")
	addReqRecorder := newRequestRecorder(addReq, mux)
	assert.Equal(t, http.StatusBadRequest, addReqRecorder.Code)
	assert.Equal(t, "UserID should be provided", addReqRecorder.Body.String())
}

// Mocks a handler and returns a httptest.ResponseRecorder
func newRequestRecorder(req *http.Request, mux *http.ServeMux) *httptest.ResponseRecorder {
	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	mux.ServeHTTP(rr, req)
	return rr
}

func initializeHandlers(producerMock *components.Producer) *http.ServeMux {
	mux := http.NewServeMux()

	server := Server{
		Mux:     mux,
		Producer: producerMock,
	}
	mux.HandleFunc("/events", server.events)
	return mux
}