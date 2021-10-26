package pggit

import (
	"testing"

	. "github.com/iostrovok/check"
)

type testSuite struct{}

var _ = Suite(&testSuite{})

func TestService(t *testing.T) { TestingT(t) }

// test syntax only
func (s *testSuite) Test(c *C) {
	c.Assert(true, Equals, true)
}
