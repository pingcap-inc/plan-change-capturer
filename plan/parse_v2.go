package plan

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

func ParseV2(SQL string, rows [][]string) (Plan, error) {
	p := Plan{SQL: SQL, Ver: V2}
	root, err := parseV2Op(rows, 0)
	p.Root = root
	return p, err
}

func parseV2Op(rows [][]string, rowNo int) (Operator, error) {
	children := make([]Operator, 0, 2)
	childRowNo := findChildRowNo(rows, rowNo, 0)
	for _, no := range childRowNo {
		child, err := parseV2Op(rows, no)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	op, err := parseLineV2(rows[rowNo], children)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func parseLineV2(cols []string, children []Operator) (Operator, error) {
	estRows, err := strconv.ParseFloat(strings.TrimSpace(cols[1]), 64)
	if err != nil {
		return nil, err
	}
	opID := extractOperatorID(cols[0])
	opType := MatchOpType(opID)
	if OpTypeIsJoin(opType) {
		if err := adjustJoinChildrenV2(cols[3], children); err != nil {
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
	case OpTypeHashAgg:
		return HashAggOp{base}, nil
	case OpTypeStreamAgg:
		return StreamAggOp{base}, nil
	}
	return nil, errors.New("unknown operator type")
}

func adjustJoinChildrenV2(info string, children []Operator) error {
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
