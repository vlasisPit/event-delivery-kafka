package utils

import "net/http"

func ConstructErrorResponse(writer http.ResponseWriter, errorMessage string, serverError int) {
	writer.WriteHeader(serverError)
	writer.Write([]byte(errorMessage))
}

func ConstructSuccessfulResponse(writer http.ResponseWriter, statusCode int, jsonBytes []byte) {
	writer.Header().Add("content-type", "application/json")
	writer.WriteHeader(statusCode)
	if jsonBytes!=nil {
		writer.Write(jsonBytes)
	}
}
