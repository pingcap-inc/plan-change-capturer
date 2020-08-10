package plan

import (
	. "github.com/pingcap/check"
)

var explainV3SQL = `explain select * from t t1, (select ta.b, tb.a from t ta, t tb where ta.b=tb.a) t2 where t1.a=t2.b`

var explainV3Result = `
+------------------------------+----------+------+----------------------------------------------------------------------+
| id                           | count    | task | operator info                                                        |
+------------------------------+----------+------+----------------------------------------------------------------------+
| HashLeftJoin_17              | 15609.38 | root | inner join, inner:IndexReader_36, equal:[eq(test.ta.b, test.tb.a)]   |
| ├─HashLeftJoin_24            | 12487.50 | root | inner join, inner:TableReader_28, equal:[eq(test.t1.a, test.ta.b)]   |
| │ ├─TableReader_31           | 9990.00  | root | data:Selection_30                                                    |
| │ │ └─Selection_30           | 9990.00  | cop  | not(isnull(test.t1.a))                                               |
| │ │   └─TableScan_29         | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo          |
| │ └─TableReader_28           | 9990.00  | root | data:Selection_27                                                    |
| │   └─Selection_27           | 9990.00  | cop  | not(isnull(test.ta.b))                                               |
| │     └─TableScan_26         | 10000.00 | cop  | table:ta, range:[-inf,+inf], keep order:false, stats:pseudo          |
| └─IndexReader_36             | 9990.00  | root | index:IndexScan_35                                                   |
|   └─IndexScan_35             | 9990.00  | cop  | table:tb, index:a, range:[-inf,+inf], keep order:false, stats:pseudo |
+------------------------------+----------+------+----------------------------------------------------------------------+
`

func (s *parseTestSuite) TestParseV3(c *C) {
	p, err := ParseText(explainV3SQL, explainV3Result)
	c.Assert(err, IsNil)
	c.Assert(p.SQL, Equals, explainV3SQL)
	c.Assert(p.Root.ID(), Equals, "HashLeftJoin_17")
	c.Assert(len(p.Root.Children()), Equals, 2)
	c.Assert(p.Root.Children()[0].ID(), Equals, "HashLeftJoin_24")
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "TableReader_31")
}
