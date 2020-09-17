package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/pingcap/parser"
	"github.com/qw4990/plan-change-capturer/plan"
	"github.com/spf13/cobra"
)

type captureOpt struct {
	db1, db2   tidbAccessOptions
	mode       string
	queryFile  string
	schemaDir  string
	DB         string
	digestMode bool
	tables     []string
}

func newCaptureCmd() *cobra.Command {
	var opt captureOpt
	cmd := &cobra.Command{
		Use:   "capture",
		Short: "capture plan changes",
		Long:  `capture plan changes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			opt.mode = strings.ToLower(opt.mode)
			switch opt.mode {
			case "online":
				return runCaptureOnlineMode(&opt)
			case "offline":
				return runCaptureOfflineMode(&opt)
			default:
				return fmt.Errorf("unknown capture mode %v", opt.mode)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&opt.db1.addr, "addr1", "", "address of the first TiDB")
	cmd.Flags().StringVar(&opt.db1.port, "port1", "", "port of the first TiDB")
	cmd.Flags().StringVar(&opt.db1.user, "user1", "", "user name to access the first TiDB")
	cmd.Flags().StringVar(&opt.db1.password, "password1", "", "password to access the first TiDB")
	cmd.Flags().StringVar(&opt.db1.version, "ver1", "", "version of the first TiDB")
	cmd.Flags().StringVar(&opt.db2.addr, "addr2", "", "address of the second TiDB")
	cmd.Flags().StringVar(&opt.db2.port, "port2", "", "port of the second TiDB")
	cmd.Flags().StringVar(&opt.db2.user, "user2", "", "user name to access the second TiDB")
	cmd.Flags().StringVar(&opt.db2.password, "password2", "", "password to access the second TiDB")
	cmd.Flags().StringVar(&opt.db2.version, "ver2", "", "version of the second TiDB")
	cmd.Flags().StringVar(&opt.queryFile, "queryfile", "", "query file path")
	cmd.Flags().StringVar(&opt.DB, "db", "mysql", "the default database used when connecting to TiDB")
	cmd.Flags().StringVar(&opt.mode, "mode", "", "online / offline")
	cmd.Flags().StringVar(&opt.schemaDir, "dir", "", "dir to store schemas and stats")
	cmd.Flags().BoolVar(&opt.digestMode, "digestmode", false, "SQLs with the same digest only be printed once if it is true")
	cmd.Flags().StringSliceVar(&opt.tables, "tables", nil, "tables to export")
	return cmd
}

func runCaptureOfflineMode(opt *captureOpt) error {
	db1, err := startAndConnectDB(opt.db1, opt.DB)
	if err != nil {
		return fmt.Errorf("start and connect to DB1 error: %v", err)
	}
	db2, err := startAndConnectDB(opt.db1, opt.DB)
	if err != nil {
		return fmt.Errorf("start and connect to DB2 error: %v", err)
	}
	sqls, err := scanQueryFile(opt.queryFile)
	if err != nil {
		return err
	}
	return capturePlanChanges(db1, db2, sqls, opt.digestMode)
}

func runCaptureOnlineMode(opt *captureOpt) error {
	db1, err := connectDB(opt.db1, opt.DB)
	if err != nil {
		return fmt.Errorf("connect to DB1 error: %v", err)
	}
	db2, err := startAndConnectDB(opt.db2, opt.DB)
	if err != nil {
		return fmt.Errorf("start and connect to DB2 error: %v", err)
	}

	dir := tmpPathDir()
	if err := os.MkdirAll(dir, 0776); err != nil {
		return fmt.Errorf("create destination directory error: %v", err)
	}
	if err := exportSchemaStats(db1, dir, nil); err != nil {
		return fmt.Errorf("export schema and stats from DB1 error: %v", err)
	}
	if err := importSchemaStats(db2, dir); err != nil {
		return fmt.Errorf("import shcema and stats into DB2 error: %v", err)
	}

	sqls, err := scanQueryFile(opt.queryFile)
	if err != nil {
		return err
	}
	return capturePlanChanges(db1, db2, sqls, opt.digestMode)
}

func capturePlanChanges(db1, db2 *tidbHandler, sqls []string, digestMode bool) error {
	digests := make(map[string]struct{})
	for _, sql := range sqls {
		if matchPrefixCaseInsensitive(sql, "use") {
			if _, err := db1.db.Exec(sql); err != nil {
				return err
			}
			if _, err := db2.db.Exec(sql); err != nil {
				return err
			}
		} else if matchPrefixCaseInsensitive(sql, "explain") {
			_, digest := parser.NormalizeDigest(sql)
			if digestMode {
				if _, ok := digests[digest]; ok {
					continue
				}
			}

			var p1, p2 plan.Plan
			var r1, r2 [][]string
			var err error
			r1, err = runExplain(db1, sql)
			if err != nil {
				fmt.Printf("run %v on db1 err=%v\n", sql, err)
				continue
			}
			r2, err = runExplain(db2, sql)
			if err != nil {
				fmt.Printf("run %v on db2 err=%v\n", sql, err)
				continue
			}
			p1, err = plan.Parse(db1.opt.version, sql, r1)
			if err != nil {
				fmt.Printf("parse %v err=%v\n", sql, err)
				continue
			}
			p2, err = plan.Parse(db2.opt.version, sql, r2)
			if err != nil {
				fmt.Printf("parse %v err=%v\n", sql, err)
				continue
			}
			if reason, same := plan.Compare(p1, p2); !same {
				fmt.Println("=====================================================================")
				fmt.Println("SQL: ")
				fmt.Println(sql)
				fmt.Println()
				fmt.Println("Plan1: ")
				fmt.Println(plan.FormatExplainRows(r1))
				fmt.Println()
				fmt.Println("Plan2: ")
				fmt.Println(plan.FormatExplainRows(r2))
				fmt.Println()
				fmt.Println("Reason: ", reason)
				fmt.Println("=====================================================================")

				if digestMode {
					digests[digest] = struct{}{}
				}
			}
		} else {
			return fmt.Errorf("unexpected SQL %v", sql)
		}
	}
	return nil
}

func runExplain(h *tidbHandler, explainSQL string) ([][]string, error) {
	rows, err := h.db.Query(explainSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	nCols := len(types)
	results := make([][]string, 0, 8)
	for rows.Next() {
		cols := make([]string, nCols)
		ptrs := make([]interface{}, nCols)
		for i := 0; i < nCols; i++ {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		results = append(results, cols)
	}
	return results, nil
}

func scanQueryFile(filepath string) ([]string, error) {
	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	sqls := strings.Split(string(data), ";")
	i := 0
	for _, sql := range sqls {
		sql = strings.TrimSpace(sql)
		if len(sql) == 0 {
			continue
		}

		if matchPrefixCaseInsensitive(sql, "select") {
			sql = "explain " + sql
		} else if matchPrefixCaseInsensitive(sql, "explain analyze select") {
			sql = "explain " + sql[len("explain analyze "):]
		} else if matchPrefixCaseInsensitive(sql, "explain select") {
			// do nothing
		} else if matchPrefixCaseInsensitive(sql, "use") {
			// change database, do nothing
		} else {
			continue // ignore all DML SQLs
		}

		sqls[i] = sql + ";"
		i++
	}
	return sqls[:i], nil
}

func matchPrefixCaseInsensitive(sql, prefix string) bool {
	if len(sql) < len(prefix) {
		return false
	}
	return strings.ToLower(sql[:len(prefix)]) == strings.ToLower(prefix)
}
