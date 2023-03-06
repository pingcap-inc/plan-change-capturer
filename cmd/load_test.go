package cmd

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&loadTestSuite{})

type loadTestSuite struct{}

func (s *loadTestSuite) TestParseLoadPlan(c *C) {
	plans, err := loadSQLsAndPlans(nil, "./testdata/extract_testdata.zip")
	c.Check(err, IsNil)
	for _, plan := range plans {
		c.Assert(plan.Root, NotNil)
	}
}

func (s *loadTestSuite) TestRunExplain(c *C) {
	var opt tidbAccessOptions
	opt.version = "v6.0.0"
	opt.port = "4000"
	opt.statusPort = "10080"
	db1, err := startAndConnectDB(opt, "test")
	if err != nil {
		fmt.Println()
	}
	db1.execute("use test")
	db1.execute("create table t(id int)")
	explainRows, _ := runExplain(db1, "explain select * from t")
	fmt.Println("here here")
	for _, rows := range explainRows {
		fmt.Println(len(rows))
		fmt.Println(strings.Join(rows, "\t"))
	}
}
