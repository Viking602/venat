package message

import "testing"

func TestCheckJSON_Boundaries(t *testing.T) {
	limits := JSONLimits{Bytes: 32, Depth: 3, Values: 4, Collection: 2}
	for _, test := range []struct {
		name  string
		data  string
		valid bool
	}{
		{"exact array", `[1,2]`, true},
		{"exact depth", `[[0]]`, true},
		{"exact values", `[[0],1]`, true},
		{"too many elements", `[1,2,3]`, false},
		{"too many members", `{"a":1,"b":2,"c":3}`, false},
		{"too deep", `[[[0]]]`, false},
		{"too many values", `[[0],[1]]`, false},
		{"duplicate", `{"a":1,"a":2}`, false},
		{"escaped duplicate", `{"a":1,"\u0061":2}`, false},
		{"trailing", `{} []`, false},
		{"unterminated", `{"a":[1,2]`, false},
		{"too large", `"123456789012345678901234567890123"`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := CheckJSON([]byte(test.data), limits)
			if (err == nil) != test.valid {
				t.Fatalf("CheckJSON(%s) = %v, valid=%v", test.data, err, test.valid)
			}
		})
	}
}
