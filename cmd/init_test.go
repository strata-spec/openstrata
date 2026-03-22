package cmd

import (
	"errors"
	"testing"
)

func TestIsMultilineError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "single-line error",
			err:  errors.New("single line"),
			want: false,
		},
		{
			name: "multiline error",
			err:  errors.New("line one\nline two"),
			want: true,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isMultilineError(tt.err)
			if got != tt.want {
				t.Fatalf("isMultilineError() = %v, want %v", got, tt.want)
			}
		})
	}
}
