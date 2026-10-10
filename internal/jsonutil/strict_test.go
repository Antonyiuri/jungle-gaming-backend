package jsonutil

import "testing"

func TestStrictJSON(t *testing.T) {
	for _, input := range []string{`{"x":"a","x":"b"}`, `{"x":"\ud800"}`, `{"x":"\udc00"}`, `{"x":"ok"} {}`, `{"unknown":"a"}`} {
		var value struct {
			X string `json:"x"`
		}
		if err := Decode([]byte(input), &value); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
	var value struct {
		X string `json:"x"`
	}
	if err := Decode([]byte(`{"x":"\ud83d\ude00"}`), &value); err != nil {
		t.Fatal(err)
	}
}
