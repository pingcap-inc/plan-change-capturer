package plan

import (
	. "github.com/pingcap/check"
)

var explainV2SQL = `explain select * from t t1, (select ta.b, tb.a from t ta, t tb where ta.b=tb.a) t2 where t1.a=t2.b`

var explainV2Result = `
+--------------------------+----------+------+--------------------------------------------------------------------+
| id                       | count    | task | operator info                                                      |
+--------------------------+----------+------+--------------------------------------------------------------------+
| HashRightJoin_14         | 15625.00 | root | inner join, inner:TableReader_16, equal:[eq(test.t1.a, test.ta.b)] |
| ├─TableReader_16         | 10000.00 | root | data:TableScan_15                                                  |
| │ └─TableScan_15         | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo        |
| └─HashLeftJoin_17        | 12500.00 | root | inner join, inner:TableReader_22, equal:[eq(test.ta.b, test.tb.a)] |
|   ├─TableReader_20       | 10000.00 | root | data:TableScan_19                                                  |
|   │ └─TableScan_19       | 10000.00 | cop  | table:ta, range:[-inf,+inf], keep order:false, stats:pseudo        |
|   └─TableReader_22       | 10000.00 | root | data:TableScan_21                                                  |
|     └─TableScan_21       | 10000.00 | cop  | table:tb, range:[-inf,+inf], keep order:false, stats:pseudo        |
+--------------------------+----------+------+--------------------------------------------------------------------+
`

func (s *parseTestSuite) TestParseV2(c *C) {
	p, err := ParseText(explainV2SQL, explainV2Result, V2)
	c.Assert(err, IsNil)
	c.Assert(p.SQL, Equals, explainV2SQL)
	c.Assert(p.Root.ID(), Equals, "HashRightJoin_14")
	c.Assert(len(p.Root.Children()), Equals, 2)
	c.Assert(p.Root.Children()[0].ID(), Equals, "HashLeftJoin_17")
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "TableReader_20")
}
