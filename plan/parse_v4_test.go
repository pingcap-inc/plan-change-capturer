package plan

import (
	. "github.com/pingcap/check"
)

var explainV4SQL = `explain select * from t t1, (select ta.b, tb.a from t ta, t tb where ta.b=tb.a) t2 where t1.a=t2.b`

var explainV4Result = `
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
| id                             | estRows  | task      | access object        | operator info                              |
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
| HashJoin_24                    | 15609.38 | root      |                      | inner join, equal:[eq(test.t.b, test.t.a)] |
| ├─IndexReader_52(Build)        | 9990.00  | root      |                      | index:IndexFullScan_51                     |
| │ └─IndexFullScan_51           | 9990.00  | cop[tikv] | table:tb, index:a(a) | keep order:false, stats:pseudo             |
| └─HashJoin_40(Probe)           | 12487.50 | root      |                      | inner join, equal:[eq(test.t.a, test.t.b)] |
|   ├─TableReader_44(Build)      | 9990.00  | root      |                      | data:Selection_43                          |
|   │ └─Selection_43             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.b))                      |
|   │   └─TableFullScan_42       | 10000.00 | cop[tikv] | table:ta             | keep order:false, stats:pseudo             |
|   └─TableReader_47(Probe)      | 9990.00  | root      |                      | data:Selection_46                          |
|     └─Selection_46             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.a))                      |
|       └─TableFullScan_45       | 10000.00 | cop[tikv] | table:t1             | keep order:false, stats:pseudo             |
+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
`

func (s *parseTestSuite) TestParseV4(c *C) {
	p, err := ParseText(explainV4SQL, explainV4Result)
	c.Assert(err, IsNil)
	c.Assert(p.SQL, Equals, explainV4SQL)
	c.Assert(p.Root.ID(), Equals, "HashJoin_24")
	c.Assert(len(p.Root.Children()), Equals, 2)
	c.Assert(p.Root.Children()[0].ID(), Equals, "IndexReader_52(Build)")
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "IndexFullScan_51")
}
