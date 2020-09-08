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
	p, err := ParseText(explainV4SQL, explainV4Result, V4)
	c.Assert(err, IsNil)
	c.Assert(p.SQL, Equals, explainV4SQL)
	c.Assert(p.Root.ID(), Equals, "HashJoin_24")
	c.Assert(len(p.Root.Children()), Equals, 2)
	c.Assert(p.Root.Children()[0].ID(), Equals, "IndexReader_52(Build)")
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "IndexFullScan_51")
}

func (s *parseTestSuite) TestParsePointGetV4(c *C) {
	result := `
+-------------------+---------+------+---------------+----------------------------------------------+
| id                | estRows | task | access object | operator info                                |
+-------------------+---------+------+---------------+----------------------------------------------+
| Batch_Point_Get_1 | 3.00    | root | table:t       | handle:[3 4 5], keep order:false, desc:false |
+-------------------+---------+------+---------------+----------------------------------------------+
`
	_, err := ParseText("", result, V4)
	c.Assert(err, IsNil)
}

func (s *parseTestSuite) TestParseAggV4(c *C) {
	p1 := `
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
| id                        | estRows  | task      | access object | operator info                                    |
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
| HashAgg_9                 | 8000.00  | root      |               | group by:test.t.b, funcs:sum(Column#5)->Column#4 |
| └─TableReader_10          | 8000.00  | root      |               | data:HashAgg_5                                   |
|   └─HashAgg_5             | 8000.00  | cop[tikv] |               | group by:test.t.b, funcs:sum(test.t.a)->Column#5 |
|     └─TableFullScan_8     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo                   |
+---------------------------+----------+-----------+---------------+--------------------------------------------------+
`
	p, err := ParseText("", p1, V4)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "HashAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeHashAgg)
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "HashAgg_5")
	c.Assert(p.Root.Children()[0].Children()[0].Type(), Equals, OpTypeHashAgg)

	p2 := `
+--------------------------------------+----------+-----------+---------------------+-----------------------------------------------------------+
| id                                   | estRows  | task      | access object       | operator info                                             |
+--------------------------------------+----------+-----------+---------------------+-----------------------------------------------------------+
| StreamAgg_9                          | 8000.00  | root      |                     | group by:Column#12, funcs:sum(Column#11)->Column#4        |
| └─Projection_28                      | 10000.00 | root      |                     | cast(test.t.b, decimal(65,0) BINARY)->Column#11, test.t.a |
|   └─Projection_16                    | 10000.00 | root      |                     | test.t.a, test.t.b                                        |
|     └─IndexLookUp_15                 | 10000.00 | root      |                     |                                                           |
|       ├─IndexFullScan_13(Build)      | 10000.00 | cop[tikv] | table:t, index:a(a) | keep order:true, stats:pseudo                             |
|       └─TableRowIDScan_14(Probe)     | 10000.00 | cop[tikv] | table:t             | keep order:false, stats:pseudo                            |
+--------------------------------------+----------+-----------+---------------------+-----------------------------------------------------------+
`
	p, err = ParseText("", p2, V4)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "StreamAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeStreamAgg)
	c.Assert(p.Root.Children()[0].Children()[0].Children()[0].ID(), Equals, "IndexLookUp_15")
}
