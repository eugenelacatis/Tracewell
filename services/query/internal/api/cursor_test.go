package api

import (
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	in := Cursor{
		StartTime: time.Date(2026, 10, 26, 17, 4, 5, 123456000, time.UTC),
		TraceID:   "4bf92f3577b34da6a3ce929d0e0e4736",
	}
	out, err := DecodeCursor(EncodeCursor(in))
	if err != nil {
		t.Fatal(err)
	}
	if !out.StartTime.Equal(in.StartTime) || out.TraceID != in.TraceID {
		t.Fatalf("round trip changed cursor: %+v -> %+v", in, out)
	}
}

func TestDecodeContractExample(t *testing.T) {
	c, err := DecodeCursor("eyJ0IjoiMjAyNi0xMC0yNlQxNzowNDowNVoiLCJpZCI6IjRiZjkuLi4ifQ")
	if err != nil {
		t.Fatal(err)
	}
	if c.TraceID != "4bf9..." || !c.StartTime.Equal(time.Date(2026, 10, 26, 17, 4, 5, 0, time.UTC)) {
		t.Fatalf("got %+v", c)
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	for _, s := range []string{"%%%", "bm90IGpzb24", "eyJ0Ijoibm9wZSIsImlkIjoieCJ9", "eyJ0IjoiMjAyNi0xMC0yNlQxNzowNDowNVoifQ"} {
		if _, err := DecodeCursor(s); err == nil {
			t.Errorf("DecodeCursor(%q) should fail", s)
		}
	}
}

func TestParseLimit(t *testing.T) {
	cases := []struct {
		in   string
		want int
		err  bool
	}{
		{"", 50, false},
		{"10", 10, false},
		{"200", 200, false},
		{"201", 200, false},
		{"100000", 200, false},
		{"0", 1, false},
		{"-5", 1, false},
		{"abc", 0, true},
	}
	for _, c := range cases {
		got, err := ParseLimit(c.in)
		if (err != nil) != c.err || got != c.want {
			t.Errorf("ParseLimit(%q) = %d, %v; want %d, err=%v", c.in, got, err, c.want, c.err)
		}
	}
}
