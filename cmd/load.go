package cmd

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	"github.com/qw4990/plan-change-capturer/plan"
	"github.com/spf13/cobra"
)

type loadOpt struct {
	db1        tidbAccessOptions
	path       string
	targetFile string
}

func newLoadCmd() *cobra.Command {
	var opt loadOpt
	cmd := &cobra.Command{
		Use:   "load-and-compare",
		Short: "capture plan changes",
		Long:  `capture plan changes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadCompareInOfflineMode(&opt)
		},
	}
	cmd.Flags().StringVar(&opt.db1.addr, "addr", "127.0.0.1", "address of the first TiDB")
	cmd.Flags().StringVar(&opt.db1.port, "port", "4000", "port of the first TiDB")
	cmd.Flags().StringVar(&opt.db1.statusPort, "status-port", "10080", "status port of the first TiDB")
	cmd.Flags().StringVar(&opt.db1.version, "version", "", "version for new tidb")
	cmd.Flags().StringVar(&opt.path, "path", "", "path for package")
	cmd.Flags().StringVar(&opt.targetFile, "target-file", "", "target file path")
	return cmd
}

func runLoadCompareInOfflineMode(opt *loadOpt) error {
	var db1 *tidbHandler
	var err error
	if len(opt.path) < 1 {
		return fmt.Errorf("pcc packge should be given")
	}
	paths := strings.Split(opt.path, ",")

	zrs, err := loadAllPackages(paths)
	if err != nil {
		return err
	}
	plans, err := parsePlanFromPackages(zrs)
	if err != nil {
		return err
	}
	db1, err = startAndConnectDB(opt.db1, "test")
	if err != nil {
		return fmt.Errorf("start and connect to DB error: %v", err)
	}
	fmt.Println("start and connect tidb success")
	defer func() {
		db1.stop()
		fmt.Println("database closed")
	}()
	if err := importAllSchemaAndStats(db1, zrs); err != nil {
		return err
	}
	newPlans, err := explainSQLsAndCompare(db1, plans)
	if err != nil {
		fmt.Println("explain sqls failed, err:", err.Error())
		return err
	}
	fmt.Println("explain sqls and compare success")
	result := comparePlan(plans, newPlans, db1.opt.version)
	if err := dumpResultsIntoTargetFile(opt.targetFile, result); err != nil {
		fmt.Println("dump result failed, err:", err.Error())
	}
	fmt.Println("dump result success")
	return nil
}

func loadAllPackages(paths []string) ([]*zip.Reader, error) {
	zrs := make([]*zip.Reader, 0)
	for _, path := range paths {
		zr, err := loadExtractPlanPackage(path)
		if err != nil {
			return nil, err
		}
		zrs = append(zrs, zr)
	}
	return zrs, nil
}

func parsePlanFromPackages(zrs []*zip.Reader) ([]plan.Plan, error) {
	plans := make([]plan.Plan, 0)
	for _, zr := range zrs {
		subPlans, err := loadSQLsAndPlans(zr, "")
		if err != nil {
			return nil, err
		}
		plans = append(plans, subPlans...)
	}
	return plans, nil
}

func explainSQLsAndCompare(db *tidbHandler, originPlans []plan.Plan) ([]plan.Plan, error) {
	newPlans := make([]plan.Plan, 0)
	for _, originPlan := range originPlans {
		if err := db.execute(fmt.Sprintf("use `%s`", originPlan.Schema)); err != nil {
			return nil, err
		}
		explainRows, err := runExplain(db, fmt.Sprintf("explain %v", originPlan.SQL))
		if err != nil {
			return nil, err
		}
		p, err := plan.Parse(plan.V4, originPlan.SQL, explainRows)
		if err != nil {
			return nil, err
		}
		p.Schema = originPlan.Schema
		p.PlanText = getPlanText(explainRows)
		newPlans = append(newPlans, p)
	}
	return newPlans, nil
}

func getPlanText(explainRows [][]string) string {
	rows := make([]string, 0)
	for _, eRows := range explainRows {
		rows = append(rows, strings.Join(eRows, "\t"))
	}
	return strings.Join(rows, "\n")
}

func importAllSchemaAndStats(db *tidbHandler, zrs []*zip.Reader) error {
	for _, zr := range zrs {
		if err := importSchemaFromExtractPlan(db, zr); err != nil {
			fmt.Println("import schema failed, err:", err.Error())
			return err
		}
		fmt.Println("import schema success")
		if err := importStatsFromExtractPlan(db, zr); err != nil {
			fmt.Println("import stats failed, err:", err.Error())
			return err
		}
		fmt.Println("import stats success")
	}
	return nil
}

func dumpResultsIntoTargetFile(targetFile string, result PlanCompareResult) error {
	content, err := json.Marshal(&result)
	if err != nil {
		return err
	}
	return os.WriteFile(targetFile, content, 0666)
}

type PlanCompareResult struct {
	NewVersion string                    `json:"newVersion"`
	Results    []SinglePlanCompareResult `json:"results"`
}

type SinglePlanCompareResult struct {
	SQL        string `json:"sql"`
	Digest     string `json:"digest"`
	Schema     string `json:"schema"`
	OldPlan    string `json:"oldPlan"`
	NewPlan    string `json:"newPlan"`
	NewVersion string
	Same       bool   `json:"same"`
	Reason     string `json:"reason"`
}

func comparePlan(oldPlans, newPlans []plan.Plan, version string) PlanCompareResult {
	result := PlanCompareResult{}
	rs := make([]SinglePlanCompareResult, 0)
	for i, oldPlan := range oldPlans {
		newPlan := newPlans[i]
		reason, same := plan.Compare(oldPlan, newPlan)
		r := SinglePlanCompareResult{
			SQL:        oldPlan.SQL,
			Schema:     oldPlan.Schema,
			OldPlan:    oldPlan.PlanText,
			NewPlan:    newPlan.PlanText,
			NewVersion: version,
			Same:       same,
			Reason:     reason,
		}
		// If they have same plan, then we only record plan once
		if same {
			r.NewPlan = ""
		}
		rs = append(rs, r)
	}
	result.Results = rs
	result.NewVersion = rs[0].NewVersion
	return result
}

func loadSQLsAndPlans(zr *zip.Reader, path string) ([]plan.Plan, error) {
	var err error
	// used for unit test
	if zr == nil {
		zr, err = loadExtractPlanPackage(path)
		if err != nil {
			return nil, err
		}
	}
	var originPlans []plan.Plan
	for _, zipFile := range zr.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "SQLs") == 0 {
			p, err := parseSQLAndPlan(zipFile)
			if err != nil {
				return nil, err
			}
			originPlans = append(originPlans, p)
		}
	}
	return originPlans, nil
}

type singleSQLRecord struct {
	Schema string `json:"schema"`
	Plan   string `json:"plan"`
	SQL    string `json:"sql"`
	Digest string `json:"digest"`
}

func parseSQLAndPlan(zf *zip.File) (plan.Plan, error) {
	r, err := zf.Open()
	if err != nil {
		return plan.Plan{}, errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return plan.Plan{}, errors.AddStack(err)
	}
	record := singleSQLRecord{}
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		return plan.Plan{}, errors.AddStack(err)
	}
	sql := record.SQL
	newSQL := handlePreparedSQL(sql)
	planText := record.Plan
	dbName := record.Schema
	return handlePlan(planText, newSQL, dbName)
}

func handlePreparedSQL(oldSQL string) string {
	index := strings.Index(oldSQL, "[arguments:")
	if index == -1 {
		return oldSQL
	}
	lastIndex := strings.LastIndex(oldSQL, "]")
	normalizedSQL := oldSQL[:index]
	arguments := oldSQL[index : lastIndex+1]
	args := handleArguments(arguments)
	for _, arg := range args {
		normalizedSQL = strings.Replace(normalizedSQL, "?", arg, 1)
	}
	return normalizedSQL
}

func handleArguments(args string) []string {
	index := strings.Index(args, "(")
	if index == -1 {
		return []string{args[len("[arguments: ") : len(args)-1]}
	}
	lastIndex := strings.LastIndex(args, ")")
	return strings.Split(args[index+1:lastIndex], ", ")
}

func handlePlan(planText, sql, dbName string) (plan.Plan, error) {
	explainRows := make([][]string, 0)
	for index, row := range strings.Split(planText, "\n") {
		if index == 0 {
			continue
		}
		items := make([]string, 0)
		for i, item := range strings.Split(row, "|") {
			switch i {
			case 1, 2, 5, 6, 8:
				items = append(items, item)
			}
		}
		explainRows = append(explainRows, items)
	}
	p, err := plan.Parse(plan.V4, sql, explainRows)
	if err != nil {
		return plan.Plan{}, err
	}
	p.Schema = dbName
	p.PlanText = planText
	return p, nil
}

func importSchemaFromExtractPlan(db *tidbHandler, zr *zip.Reader) error {
	// build schema and table first
	for _, zipFile := range zr.File {
		if zipFile.Name == fmt.Sprintf("schema/%v", "schema_meta.txt") {
			continue
		}
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "schema") == 0 {
			if err := createSchemaAndItems(db, zipFile); err != nil {
				return err
			}
		}
	}
	// build view next
	for _, zipFile := range zr.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "view") == 0 {
			if err := createSchemaAndItems(db, zipFile); err != nil {
				return err
			}
		}
	}
	return nil
}

func importStatsFromExtractPlan(db *tidbHandler, zr *zip.Reader) error {
	dir := tmpPathDir()
	if err := os.MkdirAll(filepath.Join(dir, "stats"), 0776); err != nil {
		return fmt.Errorf("create destination directory error: %v", err)
	}
	for _, zipFile := range zr.File {
		path := strings.Split(zipFile.Name, "/")
		if len(path) == 2 && strings.Compare(path[0], "stats") == 0 {
			if err := dumpStatsAndImport(db, zipFile, dir); err != nil {
				return err
			}
		}
	}
	return nil
}

func dumpStatsAndImport(db *tidbHandler, zf *zip.File, dir string) error {
	r, err := zf.Open()
	if err != nil {
		fmt.Println("open zf failed, err:", err.Error())
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		fmt.Println("read buffer failed, err:", err.Error())
		return errors.AddStack(err)
	}
	content := buf.Bytes()
	fileName := filepath.Join(dir, zf.Name)
	if err := os.WriteFile(fileName, content, 0666); err != nil {
		fmt.Println("dump stats failed, err:", err.Error())
		return err
	}
	if err := db.execute(fmt.Sprintf("load stats '%s'", fileName)); err != nil {
		fmt.Println("execute load stats failed, err:", err.Error())
		return err
	}
	return nil
}

// createSchemaAndItems creates schema and tables or views
func createSchemaAndItems(db *tidbHandler, f *zip.File) error {
	r, err := f.Open()
	if err != nil {
		return errors.AddStack(err)
	}
	//nolint: errcheck
	defer r.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return errors.AddStack(err)
	}
	originText := buf.String()
	index1 := strings.Index(originText, ";")
	createDatabaseSQL := originText[:index1+1]
	index2 := strings.Index(originText[index1+1:], ";")
	useDatabaseSQL := originText[index1+1:][:index2+1]
	createTableSQL := originText[index1+1:][index2+1:]
	if err := db.execute(createDatabaseSQL); err != nil {
		return err
	}
	if err := db.execute(useDatabaseSQL); err != nil {
		return err
	}
	if err := db.execute(createTableSQL); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}

func loadExtractPlanPackage(path string) (*zip.Reader, error) {
	//nolint: gosec
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	b := bytes.NewReader(content)
	zr, err := zip.NewReader(b, int64(len(content)))
	if err != nil {
		return nil, err
	}
	return zr, nil
}
