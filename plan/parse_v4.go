package plan

import (
	"github.com/pingcap/errors"
	"strconv"
	"strings"
)

func ParseV4(SQL string, rows [][]string) (Plan, error) {
	p := Plan{SQL: SQL, Ver: V4}
	root, err := parseV4Op(rows, 0)
	p.Root = root
	return p, err
}

func parseV4Op(rows [][]string, rowNo int) (Operator, error) {
	children := make([]Operator, 0, 2)
	childRowNo := findChildRowNo(rows, rowNo, 0)
	for _, no := range childRowNo {
		child, err := parseV4Op(rows, no)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	op, err := parseRowV4(rows[rowNo], children)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func parseRowV4(cols []string, children []Operator) (Operator, error) {
	estRows, err := strconv.ParseFloat(strings.TrimSpace(cols[1]), 64)
	if err != nil {
		return nil, err
	}
	opID := extractOperatorID(cols[0])
	opType := MatchOpType(opID)
	if OpTypeIsJoin(opType) {
		adjustJoinChildrenV4(children)
	}
	base := BaseOp{
		id:       opID,
		opType:   opType,
		estRow:   estRows,
		task:     parseTaskType(cols[2]),
		children: children,
	}

	switch opType {
	case OpTypeHashJoin:
		return HashJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeMergeJoin:
		return MergeJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeIndexJoin:
		return IndexJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeTableReader:
		return TableReaderOp{base}, nil
	case OpTypeTableScan:
		kvs := splitKVs(cols[3])
		return TableScanOp{base, kvs["table"]}, nil
	case OpTypeIndexReader:
		return IndexReaderOp{base}, nil
	case OpTypeIndexScan:
		kvs := splitKVs(cols[3])
		return IndexScanOp{base, kvs["table"], strings.Split(kvs["index"], "(")[0]}, nil
	case OpTypeIndexLookup:
		return IndexLookupOp{base}, nil
	case OpTypeSelection:
		return SelectionOp{base}, nil
	case OpTypeProjection:
		return ProjectionOp{base}, nil
	case OpTypePointGet:
		return PointGetOp{base, false}, nil
	case OpTypeHashAgg:
		return HashAggOp{base}, nil
	case OpTypeStreamAgg:
		return StreamAggOp{base}, nil
	case OpTypeMaxOneRow:
		return MaxOneRowOp{base}, nil
	case OpTypeApply:
		return ApplyOp{base}, nil
	}
	return nil, errors.Errorf("unknown operator type %v", opID)
}

func adjustJoinChildrenV4(children []Operator) {
	// make children[0] is the outer side
	if strings.Contains(strings.ToLower(children[0].ID()), "probe") {
		children[0], children[1] = children[1], children[0]
	}
}
