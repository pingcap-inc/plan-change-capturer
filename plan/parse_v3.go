package plan

import (
	"github.com/pingcap/errors"
	"strconv"
	"strings"
)

func ParseV3(SQL string, rows [][]string) (Plan, error) {
	p := Plan{SQL: SQL, Ver: V3}
	root, err := parseV3Op(rows, 0)
	p.Root = root
	return p, err
}

func parseV3Op(rows [][]string, rowNo int) (Operator, error) {
	children := make([]Operator, 0, 2)
	childRowNo := findChildRowNo(rows, rowNo, 0)
	for _, no := range childRowNo {
		child, err := parseV3Op(rows, no)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	op, err := parseLineV3(rows[rowNo], children)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func parseLineV3(cols []string, children []Operator) (Operator, error) {
	estRows, err := strconv.ParseFloat(strings.TrimSpace(cols[1]), 64)
	if err != nil {
		return nil, err
	}
	opID := extractOperatorID(cols[0])
	opType := MatchOpType(opID)
	if OpTypeIsJoin(opType) {
		if err := adjustJoinChildrenV3(cols[3], children); err != nil {
			return nil, err
		}
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
		return IndexScanOp{base, kvs["table"], extractIndexColumns(kvs["index"])}, nil
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
	case OpTypeLimit:
		return LimitOp{base}, nil
	}
	return nil, errors.Errorf("unknown operator type %v", opID)
}

func adjustJoinChildrenV3(info string, children []Operator) error {
	// make children[0] is the outer side
	idx := strings.Index(info, "inner:")
	if idx == -1 {
		return errors.New("cannot find inner side")
	}
	idx += len("inner:")
	ed := idx + 1
	for ed < len(info) && info[ed] != ',' {
		ed++
	}
	innerName := info[idx:ed]
	if children[0].ID() == innerName {
		children[0], children[1] = children[1], children[0]
	}
	return nil
}
