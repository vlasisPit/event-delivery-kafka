package models

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestCountryDeserialization(t *testing.T) {
	eventBytes, err := ioutil.ReadFile("event.json")
	var event Event
	err = json.Unmarshal(eventBytes, &event)
	if err != nil {
		t.Errorf("Can not deserialize json to struct")
	}

	assert.Equal(t, "user_test_1", event.UserID)
	assert.Equal(t, "event click !!!!", event.Payload)
}