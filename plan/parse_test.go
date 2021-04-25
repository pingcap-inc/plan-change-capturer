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
		{
			"select * from SYS_PARAMETER_CONFIG s where s.PARAMETER_TYPE = 'YXT_TASK_CONF' and s.PARAMETER_NAME = 'D267BF'",
			`
	+-------------------------------+---------+-----------+---------------------------------------+-----------------------------------------------------------+
	| id                            | estRows | task      | access object                         | operator info                                             |
	+-------------------------------+---------+-----------+---------------------------------------+-----------------------------------------------------------+
	| IndexLookUp_11                | 0.00    | root      |                                       |                                                           |
	| ├─IndexRangeScan_8(Build)     | 0.00    | cop[tikv] | table:s, index:spc_pt(PARAMETER_TYPE) | range:["YXT_TASK_CONF","YXT_TASK_CONF"], keep order:false |
	| └─Selection_10(Probe)         | 0.00    | cop[tikv] |                                       | eq(sdyx.sys_parameter_config.parameter_name, "D267BF")    |
	|   └─TableRowIDScan_9          | 0.00    | cop[tikv] | table:s                               | keep order:false                                          |
	+-------------------------------+---------+-----------+---------------------------------------+-----------------------------------------------------------+`,
			`
	+---------------------+-------+------+------------------------------------------------------------------------------------------+
	| id                  | count | task | operator info                                                                            |
	+---------------------+-------+------+------------------------------------------------------------------------------------------+
	| IndexLookUp_11      | 0.00  | root |                                                                                          |
	| ├─IndexScan_8       | 0.00  | cop  | table:s, index:PARAMETER_TYPE, range:["YXT_TASK_CONF","YXT_TASK_CONF"], keep order:false |
	| └─Selection_10      | 0.00  | cop  | eq(sdyx.s.parameter_name, "D267BF")                                                      |
	|   └─TableScan_9     | 0.00  | cop  | table:SYS_PARAMETER_CONFIG, keep order:false                                             |
	+---------------------+-------+------+------------------------------------------------------------------------------------------+`},
		{
			``,
			`
	+-------------------+---------+------+-----------------------------------------------------------------+
	| id                |  count  | task | operator info                                                   |
	+-------------------+---------+------+-----------------------------------------------------------------+
	| Projection_4      |  63.60  | root | sbtest_pcc.sbtest1.c                                            |
	| └─IndexLookUp_10  |  63.60  | root |                                                                 |
	|   ├─IndexScan_8   |  63.60  | cop  | table:sbtest1, index:k, range:[421009,421009], keep order:false |
	|   └─TableScan_9   |  63.60  | cop  | table:sbtest1, keep order:false                                 |
	+-------------------+-------+------+-------------------------------------------------------------------+`,
			`
	+------------------------------+---------+-------------+------------------------------+------------------------------------------+
	| id                           |  count  | task        | access object                | operator info                            |
	+------------------------------+---------+-------------+------------------------------+------------------------------------------+
	| Projection_4                 |  65.68  |  root       |                              |sbtest_pcc.sbtest1.c                      |
	| └─IndexLookUp_10             |  65.68  |  root       |                              |                                          |
	|   ├─IndexRangeScan_8(Build)  |  65.68  |  cop[tikv]  | table:sbtest1, index:k_1(k)  | range:[421009,421009], keep order:false  |
	|   └─TableRowIDScan_9(Probe)  |  65.68  |  cop[tikv]  | table:sbtest1                | keep order:false                         |
	+-------------------+-------+------+---------------------------------------------------------------------------------------------+`},
		{
			`explain select * FROM sbtest10 a WHERE a.k=501828`,
			`
	+-------------------+---------+------+-----------------------------------------------------------------+
	| id                |  count  | task | operator info                                                   |
	+-------------------+---------+------+-----------------------------------------------------------------+
	| IndexLookUp_10    |  359.02 | root |                                                                 |
	| ├─IndexScan_8     |  359.02 | cop  | table:a, index:k, range:[501828,501828], keep order:false       |
	| └─TableScan_9     |  359.02 | cop  | table:sbtest10, keep order:false                                |
	+-------------------+---------+------+-----------------------------------------------------------------+`,
			`
	+----------------------------+---------+-------------+------------------------------+------------------------------------------+
	| id                         |  count  | task        | access object                | operator info                            |
	+----------------------------+---------+-------------+------------------------------+------------------------------------------+
	| IndexLookUp_10             |  359.02 |  root       |                              |                                          |
	| ├─IndexRangeScan_8(Build)  |  359.02 |  cop[tikv]  |  table:a, index:k_10(k)      | range:[501828,501828], keep order:false  |
	| └─TableRowIDScan_9(Probe)  |  359.02 |  cop[tikv]  |  table:a                     | keep order:false                         |
	+----------------------------+---------+-------------+-------------------------------------------------------------------------+
`},
		{
			`explain select * FROM sbtest10 a WHERE a.k=501828`,
			`
	+-------------------+---------+------+------------------------------------------------------------------------------------------------------+
	| id                |  count  | task | operator info                                                                                        |
	+-------------------+---------+------+------------------------------------------------------------------------------------------------------+
	| IndexLookUp_10    |  359.02 | root |                                                                                                      |
	| ├─IndexScan_8     |  359.02 | cop  | table:a, index:user_id, notice_config_id, notice_type, range:[501828,501828], keep order:false       |
	| └─TableScan_9     |  359.02 | cop  | table:sbtest10, keep order:false                                                                     |
	+-------------------+---------+------+------------------------------------------------------------------------------------------------------+`,
			`
	+----------------------------+---------+-------------+----------------------------------------------------------------------------------------+------------------------------------------+
	| id                         |  count  | task        | access object                                                                          | operator info                            |
	+----------------------------+---------+-------------+----------------------------------------------------------------------------------------+------------------------------------------+
	| IndexLookUp_10             |  359.02 |  root       |                                                                                        |                                          |
	| ├─IndexRangeScan_8(Build)  |  359.02 |  cop[tikv]  |  table:a, index:idx_notice_config_id_type(user_id, notice_config_id, notice_type)      | range:[501828,501828], keep order:false  |
	| └─TableRowIDScan_9(Probe)  |  359.02 |  cop[tikv]  |  table:a                                                                               | keep order:false                         |
	+----------------------------+---------+-------------+----------------------------------------------------------------------------------------+------------------------------------------+
`},
		{
			`select id,topic,tags,msg_keys,content,status,execute_time,delay_second,task_type,retrys,date_created,created_by,date_updated,updated_by from shop_delay_msg_task_info where task_type =02 and tags in ('LIFE_POLICY_NOTICE_TAGS','FLOW_UPLOAD_IMG_OCR_TAG','POLICY_1050_UPDATE_TAGS','ELIS_UNDER_WRITING_ORDER_TAGS','RECEIVE_BOOK_TAGS','GIFT_CALL_BACK_HDFLB_TAGS','REPORT_INSURANCE_INFO_TAGS','GIFT_CALLBACK_THIRD_AFTER_UNDER_WRITINGTAGS','GIFT_CALL_BACK_COUPONS_TAGS','USER_RISK_QUESTIONS_LOG_TAGS','PRODUCT_CREATE_BOOK_TAGS','PRODUCT_MEDICAL_PAYMENT_TAGS','GIFT_PRODUCT_ACCEPT_TAGS','GIFT_CALLBACK_TAGS','PRODUCT_MEDICAL_INSURANCE_TAGS','PRODUCT_ASYNC_UNDERWRITING_TASK','GIFT_CALLBACK_THIRD_AFTER_UNDER_WRITINGTAGS') and status in ('0','1') and retrys < 10 and execute_time < now() and topic = 'LIFE_PRODUCT_TOPIC' and date_updated > '2021-04-17 18:14:00.203' order by date_updated asc limit 5000`,
			`
	+------------------------+------------+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	| id                     |  count     | task  | operator info                                                                                                                                                                       |
	+------------------------+------------+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
	|  TopN_10               |  90.06     |  root | productdb.shop_delay_msg_task_info.date_updated:asc, offset:0, count:5000                                                                                                           |
	|  └─IndexLookUp_25      |  112.57    |  root |                                                                                                                                                                                     |  
	|    ├─IndexScan_22      |  16253.86  |  cop  | table:shop_delay_msg_task_info, index:status, tags, topic, execute_time, range:["0" "ELIS_UNDER_WRITING_ORDER_TAGS" "LIFE_PRODUCT_TOPIC" -inf,"0" "ELIS_UNDER_WRITING_ORDER_TAGS"   |
	|    └─Selection_24      |  112.57    |  cop  | gt(productdb.shop_delay_msg_task_info.date_updated, 2021-04-17 18:14:00.203000), lt(productdb.shop_delay_msg_task_info.retrys, 10)                                                  |
	|      └─TableScan_23    |  16253.86  |  cop  | table:shop_delay_msg_task_info, keep order:false                                                                                                                                    |
	+------------------------+------------+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+`,
			`
	+-------------------------------+------------+-------------+------------------------------------------------------------------------------------------------+------------------------------------------+
	| id                            |  count     | task        | access object                                                                                  | operator info                            |
	+-------------------------------+------------+-------------+------------------------------------------------------------------------------------------------+------------------------------------------+	
	| TopN_10                       |  124.69    |  root       |                                                                                                |                                          |
	| └─IndexLookUp_36              |  124.69    |  root       |                                                                                                |                                          |
	|   ├─IndexRangeScan_33(Build)  |  16253.86  |  cop[tikv]  |  table:shop_delay_msg_task_info, index:async_status_compose(status, tags, topic, execute_time) |                                          |
	|   └─Selection_35(Probe)       |  124.69    |  cop[tikv]  |                                                                                                |                                          |
	|     └─TableRowIDScan_34       |  16253.86  |  cop[tikv]  |  table:shop_delay_msg_task_info                                                                |                                          |
	+----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------+------------------------------------------+`,
		},
		{
		`explain select t.STATION_AGENT_ID,t.TRIGGER_USER_ID,t.OPEN_ID,t.ANALYSIS_TYPE,t.CONTENT_ID,t.COUNT_NUM,t.TIME_RANGE,t.BUSINESS_DATE         from (select * from INNOVATION_AGENT_INSIGHT_INFO info where info.BUSINESS_DATE >= DATE_FORMAT(now(), '%Y-%m-%d')) t         where           (                t.STATION_AGENT_ID = '903384112691054592'                                 and t.TRIGGER_USER_ID = 1173499267118338048                                                and t.OPEN_ID = ''                                                and t.CONTENT_ID = ''                             and t.ANALYSIS_TYPE = '00'           or               t.STATION_AGENT_ID = '851865861592759296'                                 and t.TRIGGER_USER_ID = 884210740869840896                                                and t.OPEN_ID = ''                                                and t.CONTENT_ID = ''                             and t.ANALYSIS_TYPE = '00'           or               t.STATION_AGENT_ID = '857934794383469568'                                 and t.TRIGGER_USER_ID = 707919072733736960                                                and t.OPEN_ID = ''                                                and t.CONTENT_ID = ''                             and t.ANALYSIS_TYPE = '00'           )`,
		`
	+------------------------+------------+-------+----------------------------------------------------------------------------------------+
	| id                     |  count     | task  | operator info                                                                          |
	+------------------------+------------+-------+----------------------------------------------------------------------------------------+
	| IndexLookUp_13         |  93405.94  |  root |                                                                                        |
	| ├─IndexScan_10         |  116757.42 |  cop  | table:info, index:business_date, range:[2021-04-21 00:00:00,+inf], keep order:false    |
	| └─Selection_12         |  93405.94  |  cop  |                                                                                        |
	|   └─TableScan_11       |  116757.42 |  cop  | table:innovation_agent_insight_info, keep order:false                                  |
	+------------------------+------------+-------+----------------------------------------------------------------------------------------+`,
		`
	+-------------------------------+-------------+-------------+-------------------------------------------------------+------------------+
	| id                            |  count      | task        | access object                                         |  operator info   |
	+-------------------------------+-------------+-------------+-------------------------------------------------------+------------------+
	| IndexLookUp_13                |  86635.94   |  root       |                                                       |                  |
	| ├─IndexRangeScan_10(Build)    |  108294.92  |  cop[tikv]  |  table:info, index:idx_iaitio_bus_date(business_date) |                  | 
	| └─Selection_12(Probe)         |  86635.94   |  cop[tikv]  |                                                       |                  | 
	|   └─TableRowIDScan_11         |  108294.92  |  cop[tikv]  |  table:info                                           |                  | 
	+-------------------------------+-------------+-------------+--------------------------------------------------------------------------+`,
		},
	}

	for _, ca := range cases {
		planv3, err := ParseText(ca.sql, ca.v3, V3)
		c.Assert(err, IsNil)
		planv4, err := ParseText(ca.sql, ca.v4, V4)
		c.Assert(err, IsNil)
		reas, same := Compare(planv3, planv4, false)
		fmt.Println(">>>> ", reas)
		c.Assert(same, IsTrue)
	}
}

func (s *parseTestSuite) TestCompareSameWithoutProj(c *C) {
	cases := []struct {
		sql string
		v3  string
		v4  string
	}{
		{
			`explain select b from t where c = 10`,
			`
	+-------------------------+----------+-----------+---------------+--------------------------------+
	| id                      | estRows  | task      | access object | operator info                  |
	+-------------------------+----------+-----------+---------------+--------------------------------+
	| TableReader_7           | 10.00    | root      |               | data:Selection_6               |
	| └─Selection_6           | 10.00    | cop[tikv] |               | eq(test.t.c, 10)               |
	|   └─TableFullScan_5     | 10000.00 | cop[tikv] | table:t       | keep order:false, stats:pseudo |
	+-------------------------+----------+-----------+---------------+--------------------------------+`,
			`
	+-----------------------+----------+------+------------------------------------------------------------+
	| id                    | count    | task | operator info                                              |
	+-----------------------+----------+------+------------------------------------------------------------+
	| Projection_4          | 10.00    | root | test.t.b                                                   |
	| └─TableReader_7       | 10.00    | root | data:Selection_6                                           |
	|   └─Selection_6       | 10.00    | cop  | eq(test.t.c, 10)                                           |
	|     └─TableScan_5     | 10000.00 | cop  | table:t, range:[-inf,+inf], keep order:false, stats:pseudo |
	+-----------------------+----------+------+------------------------------------------------------------+`},
		{
			``,
			`
	+-------------------+---------+------+-----------------------------------------------------------------+
	| id                |  count  | task | operator info                                                   |
	+-------------------+---------+------+-----------------------------------------------------------------+
	| Projection_4      |  63.60  | root | sbtest_pcc.sbtest1.c                                            |
	| └─IndexLookUp_10  |  63.60  | root |                                                                 |
	|   ├─IndexScan_8   |  63.60  | cop  | table:sbtest1, index:k, range:[421009,421009], keep order:false |
	|   └─TableScan_9   |  63.60  | cop  | table:sbtest1, keep order:false                                 |
	+-------------------+-------+------+-------------------------------------------------------------------+`,
			`
	+----------------------------+---------+-------------+------------------------------+------------------------------------------+
	| id                         |  count  | task        | access object                | operator info                            |
	+----------------------------+---------+-------------+------------------------------+------------------------------------------+
	| IndexLookUp_10             |  65.68  |  root       |                              |                                          |
	| ├─IndexRangeScan_8(Build)  |  65.68  |  cop[tikv]  | table:sbtest1, index:k_1(k)  | range:[421009,421009], keep order:false  |
	| └─TableRowIDScan_9(Probe)  |  65.68  |  cop[tikv]  | table:sbtest1                | keep order:false                         |
	+-----------------+-------+------+---------------------------------------------------------------------------------------------+`},
	}

	for _, ca := range cases {
		planv3, err := ParseText(ca.sql, ca.v3, V3)
		c.Assert(err, IsNil)
		planv4, err := ParseText(ca.sql, ca.v4, V4)
		c.Assert(err, IsNil)
		reas, same := Compare(planv3, planv4, true)
		fmt.Println(">>>> ", reas)
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
		_, same := Compare(planv3, planv4, false)
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
