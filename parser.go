package main

import (
	"fmt"
	"strings"
)

type Plan struct {
	SQL  string
	Root Operator
}

type Operator struct {
	ID          string
	EstRow      float64
	Task        string
	Index       string
	Table       string
	Range       string
	PseudoStats bool
	Order       bool

	Children []Operator
}

func Parse(SQL, explainResult string) (Plan, error) {
	explainLines, err := trimAndSplitExplainResult(explainResult)
	if err != nil {
		return Plan{}, err
	}
	version := identifyVersion(explainLines[1])
	if version == "v4" {
		return ParseV4(SQL, explainLines)
	}
	return Plan{}, fmt.Errorf("unsupported TiDB version %v", version)
}

func trimAndSplitExplainResult(explainResult string) ([]string, error) {
	lines := strings.Split(explainResult, "\n")
	var idx [3]int
	p := 0
	for i := range lines {
		if isSeparateLine(lines[i]) {
			idx[p] = i
			p++
			if p == 3 {
				break
			}
		}
	}
	if p != 3 {
		return nil, fmt.Errorf("invalid explain result")
	}
	return lines[idx[0]:idx[2]], nil
}

func isSeparateLine(line string) bool {
	for _, c := range line {
		if c != '+' && c != '-' {
			return false
		}
	}
	return true
}

func identifyVersion(header string) string {
	if strings.Contains(header, "estRows") {
		return "v4"
	}
	return "v3"
}

func line2Fields(line string) []string {
	fields := strings.Split(line, "|")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	return fields
}

func findChildLineNo(explainLines []string, parentLineNo int) []int {
	col := 0
	parentLine := explainLines[parentLineNo]
	for col = range parentLine {
		c := parentLine[col]
		if c >= 'A' && c <= 'Z' {
			break
		}
	}
	if col >= len(parentLine) {
		return nil
	}
	childLineNo := make([]int, 0, 2)
	for i := parentLineNo + 1; i < len(explainLines); i++ {
		c := []rune(explainLines[i])[col]
		if c == '├' || c == '└' {
			childLineNo = append(childLineNo, i)
		}
	}
	return childLineNo
}

/*
v3.0.x
mysql> explain select * from t where a = 1;
+-------------------+-------+------+---------------------------------------------------------------+
| id                | count | task | operator info                                                 |
+-------------------+-------+------+---------------------------------------------------------------+
| IndexLookUp_10    | 10.00 | root |                                                               |
| ├─IndexScan_8     | 10.00 | cop  | table:t, index:a, range:[1,1], keep order:false, stats:pseudo |
| └─TableScan_9     | 10.00 | cop  | table:t, keep order:false, stats:pseudo                       |
+-------------------+-------+------+---------------------------------------------------------------+
3 rows in set (0.00 sec)
*/
