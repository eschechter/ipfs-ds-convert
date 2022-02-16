package testutil_test

import (
	"testing"

	"github.com/eschechter/ipfs-ds-convert/testutil"
)

func TestNewTestRepo(t *testing.T) {
	_, cl := testutil.NewTestRepo(t, nil)
	cl(t)
}
