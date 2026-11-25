package consumer

import (
	"reflect"
	"testing"
)

func TestSplitBrokers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "localhost:9092",
			expected: []string{"localhost:9092"},
		},
		{
			input:    "broker1:9092,broker2:9092, broker3:9092",
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			input:    " ,  ",
			expected: nil,
		},
	}

	for _, tc := range tests {
		got := SplitBrokers(tc.input)
		if !reflect.DeepEqual(got, tc.expected) {
			t.Errorf("SplitBrokers(%q) = %v; expected %v", tc.input, got, tc.expected)
		}
	}
}
