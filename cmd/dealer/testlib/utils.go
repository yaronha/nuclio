package testlib

import (
	"time"
	"testing"
)

func waitMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }

func CheckErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

