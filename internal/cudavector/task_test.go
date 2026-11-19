package cudavector

import (
	"strings"
	"testing"
)

func TestCUDAVectorPayloadAndReference(t *testing.T) {
	max := strings.TrimSuffix(strings.Repeat("65535,", 64), ",")
	task, err := NewTask(nil, max)
	if err != nil {
		t.Fatal(err)
	}
	result := strings.Split(task.expected, ",")
	if len(result) != 64 || result[0] != "4294836236" || result[63] != "4294836425" {
		t.Fatalf("unexpected reference for maximum inputs: %s", task.expected)
	}
	for _, bad := range []string{"", max[:len(max)-6], max + ",0", max + "\n", "65536" + max[5:],
		"00000" + max[5:], " 1234" + max[5:], "+1234" + max[5:], "-1234" + max[5:], max[5:]} {
		if _, err := ParsePayload(bad); err == nil {
			t.Errorf("accepted invalid payload %q", bad)
		}
	}
}
