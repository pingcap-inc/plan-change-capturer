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

	px := `
+--------------------------+-------+------+----------------------------------------------------------------------------+
| id                       | count | task | operator info                                                              |
+--------------------------+-------+------+----------------------------------------------------------------------------+
| Limit_9                  | 0.53  | root | offset:0, count:1                                                          |
| └─IndexLookUp_16         | 0.53  | root |                                                                            |
|   ├─IndexScan_12         | 1.00  | cop  | table:CE, index:ENTERPRISE_ID, range:["D100QX","D100QX"], keep order:false |
|   └─Limit_15             | 0.53  | cop  | offset:0, count:1                                                          |
|     └─Selection_14       | 0.53  | cop  | eq(sdyx.ce.status, "0")                                                    |
|       └─TableScan_13     | 1.00  | cop  | table:CUS_ENTERPRISE, keep order:false                                     |
+--------------------------+-------+------+----------------------------------------------------------------------------+`
	p, err = ParseText("", px, V2)
	c.Assert(err, IsNil)
	c.Assert(p.Root.Type(), Equals, OpTypeLimit)

	px = `
+------------------------------------------------+--------+------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id                                             | count  | task | operator info                                                                                                                                                                                                                                                                                                                                                                                              |
+------------------------------------------------+--------+------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Projection_31                                  | 62.83  | root | sdyx.r.resource_id, sdyx.r.status, sdyx.r.action_url, sdyx.r.resource_name, sdyx.r.parent_id, sdyx.r.url, sdyx.r.resources_icon_url, sdyx.r.sort_code, sdyx.r.sys_id, case count(*)           when 0             then 0           else 1 end, ifnull(ifnull(rr1.can_add,"1"), "1"), ifnull(ifnull(rr2.can_delete,"1"), "1"), ifnull(ifnull(rr3.can_other,"1"), "1"), ifnull(ifnull(rr4.can_look,"1"), "1") |
| └─Apply_33                                     | 62.83  | root | left outer join, inner:MaxOneRow_115                                                                                                                                                                                                                                                                                                                                                                       |
|   ├─Apply_35                                   | 62.83  | root | left outer join, inner:MaxOneRow_102                                                                                                                                                                                                                                                                                                                                                                       |
|   │ ├─Apply_37                                 | 62.83  | root | left outer join, inner:MaxOneRow_89                                                                                                                                                                                                                                                                                                                                                                        |
|   │ │ ├─Apply_39                               | 62.83  | root | left outer join, inner:MaxOneRow_76                                                                                                                                                                                                                                                                                                                                                                        |
|   │ │ │ ├─Projection_40                        | 62.83  | root | sdyx.r.resource_id, sdyx.r.status, sdyx.r.action_url, sdyx.r.resource_name, sdyx.r.parent_id, sdyx.r.url, sdyx.r.resources_icon_url, sdyx.r.sort_code, sdyx.r.sys_id, case(eq(6_col_0, 0), 0, 1)                                                                                                                                                                                                           |
|   │ │ │ │ └─Projection_41                      | 62.83  | root | sdyx.r.resource_id, sdyx.r.status, sdyx.r.action_url, sdyx.r.resource_name, sdyx.r.parent_id, sdyx.r.url, sdyx.r.resources_icon_url, sdyx.r.sort_code, sdyx.r.sys_id, ifnull(6_col_0, 0)                                                                                                                                                                                                                   |
|   │ │ │ │   └─HashLeftJoin_42                  | 62.83  | root | left outer join, inner:HashAgg_58, equal:[eq(sdyx.r.resource_id, sdyx.rr.resource_id)]                                                                                                                                                                                                                                                                                                                     |
|   │ │ │ │     ├─TableReader_45                 | 62.83  | root | data:Selection_44                                                                                                                                                                                                                                                                                                                                                                                          |
|   │ │ │ │     │ └─Selection_44                 | 62.83  | cop  | eq(sdyx.r.sys_id, "C"), ne(sdyx.r.status, "1")                                                                                                                                                                                                                                                                                                                                                             |
|   │ │ │ │     │   └─TableScan_43               | 235.00 | cop  | table:R, range:[-inf,+inf], keep order:false                                                                                                                                                                                                                                                                                                                                                               |
|   │ │ │ │     └─HashAgg_58                     | 6.00   | root | group by:col_2, funcs:count(col_0), firstrow(col_1)                                                                                                                                                                                                                                                                                                                                                        |
|   │ │ │ │       └─IndexLookUp_59               | 6.00   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │ │ │ │         ├─IndexScan_56               | 19.00  | cop  | table:RR, index:USER_ROLE_ID, range:["RGxRpwZZJjfqW8ZRM7U","RGxRpwZZJjfqW8ZRM7U"], keep order:false                                                                                                                                                                                                                                                                                                        |
|   │ │ │ │         └─HashAgg_47                 | 6.00   | cop  | group by:sdyx.rr.resource_id, funcs:count(1), firstrow(sdyx.rr.resource_id)                                                                                                                                                                                                                                                                                                                                |
|   │ │ │ │           └─TableScan_57             | 19.00  | cop  | table:SYS_ROLES_RESOURCE, keep order:false                                                                                                                                                                                                                                                                                                                                                                 |
|   │ │ │ └─MaxOneRow_76                         | 1.00   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │ │ │   └─Projection_77                      | 0.10   | root | ifnull(sdyx.rr1.can_add, "1")                                                                                                                                                                                                                                                                                                                                                                              |
|   │ │ │     └─IndexLookUp_84                   | 0.10   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │ │ │       ├─IndexScan_81                   | 3.16   | cop  | table:RR1, index:RESOURCE_ID, range: decided by [eq(sdyx.rr1.resource_id, sdyx.r.resource_id)], keep order:false                                                                                                                                                                                                                                                                                           |
|   │ │ │       └─Selection_83                   | 0.10   | cop  | eq(sdyx.rr1.user_role_id, "RGxRpwZZJjfqW8ZRM7U")                                                                                                                                                                                                                                                                                                                                                           |
|   │ │ │         └─TableScan_82                 | 3.16   | cop  | table:SYS_ROLES_RESOURCE, keep order:false                                                                                                                                                                                                                                                                                                                                                                 |
|   │ │ └─MaxOneRow_89                           | 1.00   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │ │   └─Projection_90                        | 0.10   | root | ifnull(sdyx.rr2.can_delete, "1")                                                                                                                                                                                                                                                                                                                                                                           |
|   │ │     └─IndexLookUp_97                     | 0.10   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │ │       ├─IndexScan_94                     | 3.16   | cop  | table:RR2, index:RESOURCE_ID, range: decided by [eq(sdyx.rr2.resource_id, sdyx.r.resource_id)], keep order:false                                                                                                                                                                                                                                                                                           |
|   │ │       └─Selection_96                     | 0.10   | cop  | eq(sdyx.rr2.user_role_id, "RGxRpwZZJjfqW8ZRM7U")                                                                                                                                                                                                                                                                                                                                                           |
|   │ │         └─TableScan_95                   | 3.16   | cop  | table:SYS_ROLES_RESOURCE, keep order:false                                                                                                                                                                                                                                                                                                                                                                 |
|   │ └─MaxOneRow_102                            | 1.00   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │   └─Projection_103                         | 0.10   | root | ifnull(sdyx.rr3.can_other, "1")                                                                                                                                                                                                                                                                                                                                                                            |
|   │     └─IndexLookUp_110                      | 0.10   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|   │       ├─IndexScan_107                      | 3.16   | cop  | table:RR3, index:RESOURCE_ID, range: decided by [eq(sdyx.rr3.resource_id, sdyx.r.resource_id)], keep order:false                                                                                                                                                                                                                                                                                           |
|   │       └─Selection_109                      | 0.10   | cop  | eq(sdyx.rr3.user_role_id, "RGxRpwZZJjfqW8ZRM7U")                                                                                                                                                                                                                                                                                                                                                           |
|   │         └─TableScan_108                    | 3.16   | cop  | table:SYS_ROLES_RESOURCE, keep order:false                                                                                                                                                                                                                                                                                                                                                                 |
|   └─MaxOneRow_115                              | 1.00   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|     └─Projection_116                           | 0.10   | root | ifnull(sdyx.rr4.can_look, "1")                                                                                                                                                                                                                                                                                                                                                                             |
|       └─IndexLookUp_123                        | 0.10   | root |                                                                                                                                                                                                                                                                                                                                                                                                            |
|         ├─IndexScan_120                        | 3.16   | cop  | table:RR4, index:RESOURCE_ID, range: decided by [eq(sdyx.rr4.resource_id, sdyx.r.resource_id)], keep order:false                                                                                                                                                                                                                                                                                           |
|         └─Selection_122                        | 0.10   | cop  | eq(sdyx.rr4.user_role_id, "RGxRpwZZJjfqW8ZRM7U")                                                                                                                                                                                                                                                                                                                                                           |
|           └─TableScan_121                      | 3.16   | cop  | table:SYS_ROLES_RESOURCE, keep order:false                                                                                                                                                                                                                                                                                                                                                                 |
+------------------------------------------------+--------+------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+`
	p, err = ParseText("", px, V2)
	c.Assert(err, IsNil)
}

func (s *parseTestSuite) TestParseAggV2(c *C) {
	p1 := `
+------------------------+----------+------+--------------------------------------------------------------------+
| id                     | count    | task | operator info                                                      |
+------------------------+----------+------+--------------------------------------------------------------------+
| StreamAgg_9            | 8000.00  | root | group by:col_1, funcs:sum(col_0)                                   |
| └─Projection_21        | 10000.00 | root | cast(test.t.b), test.t.a                                           |
|   └─IndexLookUp_20     | 10000.00 | root |                                                                    |
|     ├─IndexScan_18     | 10000.00 | cop  | table:t, index:a, range:[NULL,+inf], keep order:true, stats:pseudo |
|     └─TableScan_19     | 10000.00 | cop  | table:t, keep order:false, stats:pseudo                            |
+------------------------+----------+------+--------------------------------------------------------------------+
`
	p, err := ParseText("", p1, V2)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "StreamAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeStreamAgg)
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "IndexLookUp_20")
	c.Assert(p.Root.Children()[0].Children()[0].Type(), Equals, OpTypeIndexLookup)

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
	p, err = ParseText("", p2, V2)
	c.Assert(err, IsNil)
	c.Assert(p.Root.ID(), Equals, "HashAgg_9")
	c.Assert(p.Root.Type(), Equals, OpTypeHashAgg)
	c.Assert(p.Root.Children()[0].Children()[0].ID(), Equals, "HashAgg_5")
	c.Assert(p.Root.Children()[0].Children()[0].Type(), Equals, OpTypeHashAgg)
}
