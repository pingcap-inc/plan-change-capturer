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
	p, err := ParseText(explainV3SQL, explainV3Result, V3)
	c.Assert(err, IsNil)
	c.Assert(p.SQL, Equals, explainV3SQL)
	c.Assert(p.Root.ID(), Equals, "HashLeftJoin_17")
	c.Assert(len(p.Root.Children()), Equals, 2)
	c.Assert(p.Root.Children()[0].ID(), Equals, "HashLeftJoin_24")
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "TableReader_31")
}

func (s *parseTestSuite) TestParseAggV3(c *C) {
	p1 := `
+--------------------------+----------+------+--------------------------------------------------------------------+
| id                       | count    | task | operator info                                                      |
+--------------------------+----------+------+--------------------------------------------------------------------+
| StreamAgg_9              | 8000.00  | root | group by:col_1, funcs:sum(col_0)                                   |
| └─Projection_19          | 10000.00 | root | cast(test.t.b), test.t.a                                           |
|   └─Projection_18        | 10000.00 | root | test.t.a, test.t.b                                                 |
|     └─IndexLookUp_17     | 10000.00 | root |                                                                    |
|       ├─IndexScan_15     | 10000.00 | cop  | table:t, index:a, range:[NULL,+inf], keep order:true, stats:pseudo |
|       └─TableScan_16     | 10000.00 | cop  | table:t, keep order:false, stats:pseudo                            |
+--------------------------+----------+------+--------------------------------------------------------------------+
`
	p, err := ParseText("", p1, V3)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "StreamAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeStreamAgg)
	c.Assert(p.Root.Children()[0].Children()[0].Children()[0].ID(), Equals, "IndexLookUp_17")
	c.Assert(p.Root.Children()[0].Children()[0].Children()[0].Type(), Equals, OpTypeIndexLookup)

	p2 := `
+-----------------------+----------+------+------------------------------------------------------------+
| id                    | count    | task | operator info                                              |
+-----------------------+----------+------+------------------------------------------------------------+
| HashAgg_9             | 8000.00  | root | group by:col_1, funcs:sum(col_0)                           |
| └─TableReader_10      | 8000.00  | root | data:HashAgg_5                                             |
|   └─HashAgg_5         | 8000.00  | cop  | group by:test.t.b, funcs:sum(test.t.a)                     |
|     └─TableScan_8     | 10000.00 | cop  | table:t, range:[-inf,+inf], keep order:false, stats:pseudo |
+-----------------------+----------+------+------------------------------------------------------------+
`
	p, err = ParseText("", p2, V3)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "HashAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeHashAgg)
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "HashAgg_5")
	c.Assert(p.Root.Children()[0].Children()[0].Type(), Equals, OpTypeHashAgg)
}
