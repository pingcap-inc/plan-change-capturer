package main

import (
	"strconv"
	"strings"
)

//+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
//| id                             | estRows  | task      | access object        | operator info                              |
//+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
//| HashJoin_24                    | 15609.38 | root      |                      | inner join, equal:[eq(test.t.b, test.t.a)] |
//| ├─IndexReader_52(Build)        | 9990.00  | root      |                      | index:IndexFullScan_51                     |
//| │ └─IndexFullScan_51           | 9990.00  | cop[tikv] | table:tb, index:a(a) | keep order:false, stats:pseudo             |
//| └─HashJoin_40(Probe)           | 12487.50 | root      |                      | inner join, equal:[eq(test.t.a, test.t.b)] |
//|   ├─TableReader_44(Build)      | 9990.00  | root      |                      | data:Selection_43                          |
//|   │ └─Selection_43             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.b))                      |
//|   │   └─TableFullScan_42       | 10000.00 | cop[tikv] | table:ta             | keep order:false, stats:pseudo             |
//|   └─TableReader_47(Probe)      | 9990.00  | root      |                      | data:Selection_46                          |
//|     └─Selection_46             | 9990.00  | cop[tikv] |                      | not(isnull(test.t.a))                      |
//|       └─TableFullScan_45       | 10000.00 | cop[tikv] | table:t1             | keep order:false, stats:pseudo             |
//+--------------------------------+----------+-----------+----------------------+--------------------------------------------+
func ParseV4(SQL string, explainLines []string) (Plan, error) {
	p := Plan{SQL: SQL}
	root, err := parseV4Op(explainLines, 3)
	p.Root = root
	return p, err
}

func parseV4Op(explainLines []string, curLine int) (Operator, error) {
	op, err := parseLineV4(explainLines[curLine])
	if err != nil {
		return Operator{}, err
	}
	childLineNo := findChildLineNo(explainLines, curLine)
	for _, no := range childLineNo {
		child, err := parseV4Op(explainLines, no)
		if err != nil {
			return Operator{}, err
		}
		op.Children = append(op.Children, child)
	}
	return op, nil
}

func parseLineV4(line string) (Operator, error) {
	fields := line2Fields(line)
	estRows, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return Operator{}, err
	}
	return Operator{
		ID:     strings.Trim(fields[0], "└─│"),
		EstRow: estRows,
		Task:   fields[2],
	}, nil
}
