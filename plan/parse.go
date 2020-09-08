package plan

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

func ParseText(sql, explainText, version string) (Plan, error) {
	explainLines, err := trimAndSplitExplainResult(explainText)
	if err != nil {
		return Plan{}, err
	}
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	ver := formatVersion(version)
	rows := splitRows(explainLines[3 : len(explainLines)-1])
	return Parse(ver, sql, rows)
}

func Parse(version, sql string, explainRows [][]string) (_ Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			explainContent := ""
			for _, row := range explainRows {
				explainContent += strings.Join(row, "\t") + "\n"
			}
			fmt.Printf("parse sql=%v ver=%v panic\n, explain: %v\n\n", sql, version, explainContent)
			panic(r)
		}
	}()

	switch formatVersion(version) {
	case V2:
		return ParseV2(sql, explainRows)
	case V3:
		return ParseV3(sql, explainRows)
	case V4:
		return ParseV4(sql, explainRows)
	}
	return Plan{}, errors.Errorf("unsupported TiDB version %v", version)
}

func Compare(p1, p2 Plan) (reason string, same bool) {
	if p1.SQL != p2.SQL {
		return "differentiate SQLs", false
	}
	return compare(p1.Root, p2.Root)
}

func compare(op1, op2 Operator) (reason string, same bool) {
	if op1.Type() != op2.Type() || op1.Task() != op2.Task() {
		return fmt.Sprintf("different operators %v and %v", op1.ID(), op2.ID()), false
	}
	c1, c2 := op1.Children(), op2.Children()
	if len(c1) != len(c2) {
		return fmt.Sprintf("%v and %v have different children lengths", op1.ID(), op2.ID()), false
	}
	same = true
	switch op1.Type() {
	case OpTypeTableScan:
		t1, t2 := op1.(TableScanOp), op2.(TableScanOp)
		if t1.Table != t2.Table {
			same = false
			reason = fmt.Sprintf("different table scan %v:%v, %v:%v", t1.ID(), t1.Table, t2.ID(), t2.Table)
		}
	case OpTypeIndexScan:
		t1, t2 := op1.(IndexScanOp), op2.(IndexScanOp)
		if t1.Table != t2.Table || t1.Index != t2.Index {
			same = false
			reason = fmt.Sprintf("different index scan %v:%v, %v:%v", t1.ID(), t1.Table, t2.ID(), t2.Table)
		}
	}
	if !same {
		return reason, false
	}
	for i := range c1 {
		if reason, same = compare(c1[i], c2[i]); !same {
			return reason, same
		}
	}
	return "", true
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
		return nil, errors.Errorf("invalid explain result")
	}
	return lines[idx[0] : idx[2]+1], nil
}

func isSeparateLine(line string) bool {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return false
	}
	for _, c := range line {
		if c != '+' && c != '-' {
			return false
		}
	}
	return true
}

func formatVersion(version string) string {
	version = strings.ToLower(version)
	if strings.HasPrefix(version, V2) {
		return V2
	} else if strings.HasPrefix(version, V3) {
		return V3
	}
	return V4
}

func splitRows(rows []string) [][]string {
	results := make([][]string, 0, len(rows))
	for _, row := range rows {
		cols := strings.Split(row, "|")
		cols = cols[1 : len(cols)-1]
		results = append(results, cols)
	}
	return results
}

func findChildRowNo(rows [][]string, parentRowNo, idColNo int) []int {
	parent := []rune(rows[parentRowNo][idColNo])
	col := 0
	for col = range parent {
		c := parent[col]
		if c >= 'A' && c <= 'Z' {
			break
		}
	}
	if col >= len(parent) {
		return nil
	}
	childRowNo := make([]int, 0, 2)
	for i := parentRowNo + 1; i < len(rows); i++ {
		field := rows[i][idColNo]
		if col >= len([]rune(field)) {
			break
		}
		c := []rune(field)[col]
		if c == '├' || c == '└' {
			childRowNo = append(childRowNo, i)
		} else if c != '│' {
			break
		}
	}
	return childRowNo
}

func extractOperatorID(field string) string {
	return strings.TrimFunc(field, func(c rune) bool {
		return c == '└' || c == '─' || c == '│' || c == '├' || c == ' '
	})
}

func splitKVs(kvStr string) map[string]string {
	kvMap := make(map[string]string)
	kvs := strings.Split(kvStr, ",")
	for _, kv := range kvs {
		fields := strings.Split(kv, ":")
		if len(fields) == 2 {
			kvMap[strings.TrimSpace(fields[0])] = strings.TrimSpace(fields[1])
		}
	}
	return kvMap
}

func extractIndexColumns(indexStr string) string {
	be := strings.Index(indexStr, "(")
	ed := strings.Index(indexStr, ")")
	if be != -1 && ed != -1 {
		indexStr = indexStr[be+1 : ed]
	}
	return indexStr
}

func parseTaskType(taskStr string) TaskType {
	task := strings.TrimSpace(strings.ToLower(taskStr))
	if task == "root" {
		return TaskTypeRoot
	}
	if strings.Contains(task, "tiflash") {
		return TaskTypeTiFlash
	}
	return TaskTypeTiKV
}

func MatchOpType(opID string) OpType {
	x := strings.ToLower(opID)
	if strings.Contains(x, "agg") {
		if strings.Contains(x, "hash") {
			return OpTypeHashAgg
		} else if strings.Contains(x, "stream") {
			return OpTypeStreamAgg
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "join") {
		if strings.Contains(x, "hash") {
			return OpTypeHashJoin
		} else if strings.Contains(x, "merge") {
			return OpTypeMergeJoin
		} else if strings.Contains(x, "index") {
			return OpTypeIndexJoin
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "table") {
		if strings.Contains(x, "reader") {
			return OpTypeTableReader
		} else if strings.Contains(x, "scan") {
			return OpTypeTableScan
		} else if strings.Contains(x, "dual") {
			return OpTypeTableDual
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "index") {
		if strings.Contains(x, "reader") {
			return OpTypeIndexReader
		} else if strings.Contains(x, "scan") {
			return OpTypeIndexScan
		} else if strings.Contains(x, "lookup") {
			return OpTypeIndexLookup
		}
		return OpTypeUnknown
	}
	if strings.Contains(x, "selection") {
		return OpTypeSelection
	}
	if strings.Contains(x, "projection") {
		return OpTypeProjection
	}
	if strings.Contains(x, "point") {
		return OpTypePointGet
	}
	if strings.Contains(x, "maxonerow") {
		return OpTypeMaxOneRow
	}
	if strings.Contains(x, "apply") {
		return OpTypeApply
	}
	if strings.Contains(x, "limit") {
		return OpTypeLimit
	}
	if strings.Contains(x, "sort") {
		return OpTypeSort
	}
	if strings.Contains(x, "topn") {
		return OpTypeTopN
	}
	return OpTypeUnknown
}

func FormatExplainRows(rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	nRows := len(rows)
	nCols := len(rows[0])
	fmtRows := make([]string, nRows)
	for col := 0; col < nCols; col++ {
		lengest := 0
		for i := 0; i < nRows; i++ {
			if len(fmtRows[i]) > lengest {
				lengest = len(fmtRows[i])
			}
		}
		for i := 0; i < nRows; i++ {
			gap := lengest - len(fmtRows[i])
			fmtRows[i] += strings.Repeat(" ", gap)
			if col != nCols-1 && col != 0 {
				fmtRows[i] += "  |  "
			}
			fmtRows[i] += rows[i][col]
		}
	}
	return strings.Join(fmtRows, "\n")
}
