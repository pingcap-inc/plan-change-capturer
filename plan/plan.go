package plan

import (
	"bytes"
	"strings"
)

type PlanVer string

const (
	V2       = "v2"
	V3       = "v3"
	V4       = "v4"
	VUnknown = "unknown"
)

type OpType int

const (
	OpTypeUnknown OpType = iota
	OpTypeHashJoin
	OpTypeIndexJoin
	OpTypeMergeJoin
	OpTypeHashAgg
	OpTypeStreamAgg
	OpTypeSelection
	OpTypeProjection
	OpTypeTableReader
	OpTypeTableScan
	OpTypeIndexReader
	OpTypeIndexScan
	OpTypeIndexLookup
	OpTypePointGet
	OpTypeMaxOneRow
	OpTypeApply
	OpTypeLimit
	OpTypeSort
	OpTypeTopN
	OpTypeTableDual
	OpTypeSelectLock
	OpTypeShow
)

func OpTypeIsDataSource(opType OpType) bool {
	switch opType {
	case OpTypeTableReader, OpTypeIndexReader, OpTypeIndexLookup, OpTypePointGet:
		return true
	}
	return false
}

func OpTypeIsJoin(opType OpType) bool {
	switch opType {
	case OpTypeIndexJoin, OpTypeMergeJoin, OpTypeHashJoin:
		return true
	}
	return false
}

type JoinType int

const (
	JoinTypeUnknown JoinType = iota
	JoinTypeInner
	JoinTypeLeftOuter
	JoinTypeRightOuter
	JoinTypeSemi
	JoinTypeAntiSemi
	JoinTypeLeftOuterSemi
	JoinTypeAntiLeftOuterSemi
)

type TaskType int

const (
	TaskTypeRoot TaskType = iota
	TaskTypeTiKV
	TaskTypeTiFlash
)

type Plan struct {
	Schema string
	SQL    string
	Ver    PlanVer
	Root   Operator

	PlanText string
}

func (p Plan) Format() string {
	return p.Root.Format(0)
}

type Operator interface {
	ID() string
	Type() OpType
	EstRow() float64
	Task() TaskType

	Format(indent int) string
	Children() []Operator
	SetChild(i int, child Operator)
}

type BaseOp struct {
	id     string
	opType OpType
	estRow float64
	task   TaskType

	children []Operator
}

func (op BaseOp) ID() string {
	return op.id
}

func (op BaseOp) Type() OpType {
	return op.opType
}

func (op BaseOp) EstRow() float64 {
	return op.estRow
}

func (op BaseOp) Task() TaskType {
	return op.task
}

func (op BaseOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\n")
	for _, child := range op.children {
		buf.WriteString(child.Format(indent + 4))
	}
	return buf.String()
}

func (op BaseOp) Children() []Operator {
	return op.children
}

func (op BaseOp) SetChild(i int, child Operator) {
	op.children[i] = child
}

type HashJoinOp struct {
	BaseOp
	JoinType JoinType
}

func (op HashJoinOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "build: " + op.children[0].ID() + "\n")
	for _, child := range op.children {
		buf.WriteString(child.Format(indent + 4))
	}
	return buf.String()
}

type IndexJoinOp struct {
	BaseOp
	JoinType JoinType
}

type MergeJoinOp struct {
	BaseOp
	JoinType JoinType
}

type TableReaderOp struct {
	BaseOp
}

type TableScanOp struct {
	BaseOp
	Table string
}

func (op TableScanOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\ttable:" + op.Table + "\n")
	return buf.String()
}

type IndexReaderOp struct {
	BaseOp
}

type IndexScanOp struct {
	BaseOp
	Table string
	Index string
}

func (op IndexScanOp) Format(indent int) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(strings.Repeat(" ", indent))
	buf.WriteString(op.id + "\ttable:" + op.Table + ", index:" + op.Index + "\n")
	return buf.String()
}

type IndexLookupOp struct {
	BaseOp
}

type SelectionOp struct {
	BaseOp
}

type ProjectionOp struct {
	BaseOp
}

type PointGetOp struct {
	BaseOp
	Batch bool
	Table string
}

type StreamAggOp struct {
	BaseOp
}

type HashAggOp struct {
	BaseOp
}

type MaxOneRowOp struct {
	BaseOp
}

type ApplyOp struct {
	BaseOp
}

type LimitOp struct {
	BaseOp
}

type SortOp struct {
	BaseOp
}

type TopNOp struct {
	BaseOp
}

type TableDual struct {
	BaseOp
}

type SelectLock struct {
	BaseOp
}

type ShowOp struct {
	BaseOp
}
