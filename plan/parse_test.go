package plan

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = SerialSuites(&parseTestSuite{})

type parseTestSuite struct{}

func (s *parseTestSuite) TestCompareSame(c *C) {
	cases := []struct {
		sql string
		v3  string
		v4  string
	}{
		{`explain select * from t t1, t t2 where t1.a=t2.b`,
			`
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| id                       | count    | task | operator info                                                      |
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| HashLeftJoin_13          | 12487.50 | root | inner join, inner:TableReader_17, equal:[eq(test.t1.a, test.t2.b)] |
	| ├─TableReader_20         | 9990.00  | root | data:Selection_19                                                  |
	| │ └─Selection_19         | 9990.00  | cop  | not(isnull(test.t1.a))                                             |
	| │   └─TableScan_18       | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo        |
	| └─TableReader_17         | 9990.00  | root | data:Selection_16                                                  |
	|   └─Selection_16         | 9990.00  | cop  | not(isnull(test.t2.b))                                             |
	|     └─TableScan_15       | 10000.00 | cop  | table:t2, range:[-inf,+inf], keep order:false, stats:pseudo        |
	+--------------------------+----------+------+--------------------------------------------------------------------+`,
			`
	+------------------------------+----------+-----------+---------------+--------------------------------------------+
	| id                           | estRows  | task      | access object | operator info                              |
	+------------------------------+----------+-----------+---------------+--------------------------------------------+
	| HashJoin_22                  | 12487.50 | root      |               | inner join, equal:[eq(test.t.a, test.t.b)] |
	| ├─TableReader_26(Build)      | 9990.00  | root      |               | data:Selection_25                          |
	| │ └─Selection_25             | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                      |
	| │   └─TableFullScan_24       | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo             |
	| └─TableReader_29(Probe)      | 9990.00  | root      |               | data:Selection_28                          |
	|   └─Selection_28             | 9990.00  | cop[tikv] |               | not(isnull(test.t.a))                      |
	|     └─TableFullScan_27       | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo             |
	+------------------------------+----------+-----------+---------------+--------------------------------------------+`},
		{`explain select * from t where a > 10`,
			`
	+------------------------+---------+-----------+---------------+-------------------------------------------------+
	| id                     | estRows | task      | access object | operator info                                   |
	+------------------------+---------+-----------+---------------+-------------------------------------------------+
	| TableReader_6          | 3333.33 | root      |               | data:TableRangeScan_5                           |
	| └─TableRangeScan_5     | 3333.33 | cop[tikv] | table:t       | range:(10,+inf], keep order:false, stats:pseudo |
	+------------------------+---------+-----------+---------------+-------------------------------------------------+`,
			`
	+-------------------+---------+------+----------------------------------------------------------+
	| id                | count   | task | operator info                                            |
	+-------------------+---------+------+----------------------------------------------------------+
	| TableReader_6     | 3333.33 | root | data:TableScan_5                                         |
	| └─TableScan_5     | 3333.33 | cop  | table:t, range:(10,+inf], keep order:false, stats:pseudo |
	+-------------------+---------+------+----------------------------------------------------------+`},
		{`explain select b from t where b = 10`,
			`
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| id                     | estRows | task      | access object       | operator info                                 |
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| IndexReader_6          | 10.00   | root      |                     | index:IndexRangeScan_5                        |
	| └─IndexRangeScan_5     | 10.00   | cop[tikv] | table:t, index:b(b) | range:[10,10], keep order:false, stats:pseudo |
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+`,
			`
	+-------------------+-------+------+-----------------------------------------------------------------+
	| id                | count | task | operator info                                                   |
	+-------------------+-------+------+-----------------------------------------------------------------+
	| IndexReader_6     | 10.00 | root | index:IndexScan_5                                               |
	| └─IndexScan_5     | 10.00 | cop  | table:t, index:b, range:[10,10], keep order:false, stats:pseudo |
	+-------------------+-------+------+-----------------------------------------------------------------+`},
		{`explain select * from t where b = 10`,
			`
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| id                            | estRows | task      | access object       | operator info                                 |
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| IndexLookUp_10                | 10.00   | root      |                     |                                               |
	| ├─IndexRangeScan_8(Build)     | 10.00   | cop[tikv] | table:t, index:b(b) | range:[10,10], keep order:false, stats:pseudo |
	| └─TableRowIDScan_9(Probe)     | 10.00   | cop[tikv] | table:t             | keep order:false, stats:pseudo                |
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+`,
			`
	+-------------------+-------+------+-----------------------------------------------------------------+
	| id                | count | task | operator info                                                   |
	+-------------------+-------+------+-----------------------------------------------------------------+
	| IndexLookUp_10    | 10.00 | root |                                                                 |
	| ├─IndexScan_8     | 10.00 | cop  | table:t, index:b, range:[10,10], keep order:false, stats:pseudo |
	| └─TableScan_9     | 10.00 | cop  | table:t, keep order:false, stats:pseudo                         |
	+-------------------+-------+------+-----------------------------------------------------------------+`},
		{
			`explain select b from t where c = 10`,
			`
	+---------------------------+----------+-----------+---------------+--------------------------------+
	| id                        | estRows  | task      | access object | operator info                  |
	+---------------------------+----------+-----------+---------------+--------------------------------+
	| Projection_4              | 10.00    | root      |               | test.t.b                       |
	| └─TableReader_7           | 10.00    | root      |               | data:Selection_6               |
	|   └─Selection_6           | 10.00    | cop[tikv] |               | eq(test.t.c, 10)               |
	|     └─TableFullScan_5     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo |
	+---------------------------+----------+-----------+---------------+--------------------------------+`,
			`
	+-----------------------+----------+------+------------------------------------------------------------+
	| id                    | count    | task | operator info                                              |
	+-----------------------+----------+------+------------------------------------------------------------+
	| Projection_4          | 10.00    | root | test.t.b                                                   |
	| └─TableReader_7       | 10.00    | root | data:Selection_6                                           |
	|   └─Selection_6       | 10.00    | cop  | eq(test.t.c, 10)                                           |
	|     └─TableScan_5     | 10000.00 | cop  | table:t, range:[-inf,+inf], keep order:false, stats:pseudo |
	+-----------------------+----------+------+------------------------------------------------------------+`},
	}

	for _, ca := range cases {
		planv3, err := ParseText(ca.sql, ca.v3, V3)
		c.Assert(err, IsNil)
		planv4, err := ParseText(ca.sql, ca.v4, V4)
		c.Assert(err, IsNil)
		_, same := Compare(planv3, planv4)
		c.Assert(same, IsTrue)
	}
}

func (s *parseTestSuite) TestCompareNotSame(c *C) {
	cases := []struct {
		sql string
		v3  string
		v4  string
	}{
		{`explain select * from t t1, t t2 where t1.a=t2.b`,
			`
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| id                       | count    | task | operator info                                                      |
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| HashLeftJoin_13          | 12487.50 | root | inner join, inner:TableReader_17, equal:[eq(test.t1.a, test.t2.b)] |
	| ├─TableReader_20         | 9990.00  | root | data:Selection_19                                                  |
	| │ └─Selection_19         | 9990.00  | cop  | not(isnull(test.t1.a))                                             |
	| │   └─TableScan_18       | 10000.00 | cop  | table:t2, range:[-inf,+inf], keep order:false, stats:pseudo        |
	| └─TableReader_17         | 9990.00  | root | data:Selection_16                                                  |
	|   └─Selection_16         | 9990.00  | cop  | not(isnull(test.t2.b))                                             |
	|     └─TableScan_15       | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo        |
	+--------------------------+----------+------+--------------------------------------------------------------------+`,
			`
	+------------------------------+----------+-----------+---------------+--------------------------------------------+
	| id                           | estRows  | task      | access object | operator info                              |
	+------------------------------+----------+-----------+---------------+--------------------------------------------+
	| HashJoin_22                  | 12487.50 | root      |               | inner join, equal:[eq(test.t.a, test.t.b)] |
	| ├─TableReader_26(Build)      | 9990.00  | root      |               | data:Selection_25                          |
	| │ └─Selection_25             | 9990.00  | cop[tikv] |               | not(isnull(test.t.b))                      |
	| │   └─TableFullScan_24       | 10000.00 | cop[tikv] | table:t1      | keep order:false, stats:pseudo             |
	| └─TableReader_29(Probe)      | 9990.00  | root      |               | data:Selection_28                          |
	|   └─Selection_28             | 9990.00  | cop[tikv] |               | not(isnull(test.t.a))                      |
	|     └─TableFullScan_27       | 10000.00 | cop[tikv] | table:t2      | keep order:false, stats:pseudo             |
	+------------------------------+----------+-----------+---------------+--------------------------------------------+`},
		{`explain select b from t where b = 10`,
			`
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| id                     | estRows | task      | access object       | operator info                                 |
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| IndexReader_6          | 10.00   | root      |                     | index:IndexRangeScan_5                        |
	| └─IndexRangeScan_5     | 10.00   | cop[tikv] | table:t, index:b1(b) | range:[10,10], keep order:false, stats:pseudo |
	+------------------------+---------+-----------+---------------------+-----------------------------------------------+`,
			`
	+-------------------+-------+------+-----------------------------------------------------------------+
	| id                | count | task | operator info                                                   |
	+-------------------+-------+------+-----------------------------------------------------------------+
	| IndexReader_6     | 10.00 | root | index:IndexScan_5                                               |
	| └─IndexScan_5     | 10.00 | cop  | table:t, index:b2, range:[10,10], keep order:false, stats:pseudo |
	+-------------------+-------+------+-----------------------------------------------------------------+`},
		{`explain select * from t where b = 10`,
			`
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| id                            | estRows | task      | access object       | operator info                                 |
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+
	| IndexLookUp_10                | 10.00   | root      |                     |                                               |
	| ├─IndexRangeScan_8(Build)     | 10.00   | cop[tikv] | table:t, index:b(b) | range:[10,10], keep order:false, stats:pseudo |
	| └─TableRowIDScan_9(Probe)     | 10.00   | cop[tikv] | table:t             | keep order:false, stats:pseudo                |
	+-------------------------------+---------+-----------+---------------------+-----------------------------------------------+`,
			`
	+-------------------+-------+------+-----------------------------------------------------------------+
	| id                | count | task | operator info                                                   |
	+-------------------+-------+------+-----------------------------------------------------------------+
	| IndexLookUp_10    | 10.00 | root |                                                                 |
	| ├─IndexScan_8     | 10.00 | cop  | table:t1, index:b, range:[10,10], keep order:false, stats:pseudo |
	| └─TableScan_9     | 10.00 | cop  | table:t1, keep order:false, stats:pseudo                         |
	+-------------------+-------+------+-----------------------------------------------------------------+`},
	}

	for _, ca := range cases {
		planv3, err := ParseText(ca.sql, ca.v3, V3)
		c.Assert(err, IsNil)
		planv4, err := ParseText(ca.sql, ca.v4, V4)
		c.Assert(err, IsNil)
		_, same := Compare(planv3, planv4)
		c.Assert(same, IsFalse)
	}
}

func (s *parseTestSuite) TestFormatExplainRows(c *C) {
	explainText := `
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| id                       | count    | task | operator info                                                      |
	+--------------------------+----------+------+--------------------------------------------------------------------+
	| HashLeftJoin_13          | 12487.50 | root | inner join, inner:TableReader_17, equal:[eq(test.t1.a, test.t2.b)] |
	| ├─TableReader_20         | 9990.00  | root | data:Selection_19                                                  |
	| │ └─Selection_19         | 9990.00  | cop  | not(isnull(test.t1.a))                                             |
	| │   └─TableScan_18       | 10000.00 | cop  | table:t2, range:[-inf,+inf], keep order:false, stats:pseudo        |
	| └─TableReader_17         | 9990.00  | root | data:Selection_16                                                  |
	|   └─Selection_16         | 9990.00  | cop  | not(isnull(test.t2.b))                                             |
	|     └─TableScan_15       | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo        |
	+--------------------------+----------+------+--------------------------------------------------------------------+`
	explainLines, err := trimAndSplitExplainResult(explainText)
	c.Assert(err, IsNil)
	rows := splitRows(explainLines[3 : len(explainLines)-1])
	fmt.Println(FormatExplainRows(rows))
}
