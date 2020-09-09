package cmd

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pingcap/parser"
	"github.com/qw4990/plan-change-capturer/plan"
	"github.com/spf13/cobra"
)

type captureOptions struct {
	db1       tidbAccessOptions
	db2       tidbAccessOptions
	queryFile string
}

func newCaptureCmd() *cobra.Command {
	var opt captureOptions
	var DB string
	var errMode string
	var digestMode bool
	cmd := &cobra.Command{
		Use:   "capture",
		Short: "capture plan changes",
		Long:  `capture plan changes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			errMode := strings.ToLower(errMode)
			if errMode != "exit" && errMode != "print" {
				return fmt.Errorf("invalid error mode %v", errMode)
			}

			db1, err := newDBHandler(opt.db1, DB)
			if err != nil {
				return err
			}
			db2, err := newDBHandler(opt.db2, DB)
			if err != nil {
				return err
			}
			if opt.db1.version == "" || opt.db2.version == "" {
				return fmt.Errorf("please specify TiDB versions")
			}
			sqls, err := scanQueryFile(opt.queryFile)
			if err != nil {
				return err
			}
			return capturePlanChanges(db1, db2, sqls, errMode, digestMode)
		},
	}

	cmd.Flags().StringVar(&opt.db1.addr, "addr1", "", "")
	cmd.Flags().StringVar(&opt.db1.port, "port1", "", "")
	cmd.Flags().StringVar(&opt.db1.user, "user1", "", "")
	cmd.Flags().StringVar(&opt.db1.password, "password1", "", "")
	cmd.Flags().StringVar(&opt.db1.version, "ver1", "", "")
	cmd.Flags().StringVar(&opt.db2.addr, "addr2", "", "")
	cmd.Flags().StringVar(&opt.db2.port, "port2", "", "")
	cmd.Flags().StringVar(&opt.db2.user, "user2", "", "")
	cmd.Flags().StringVar(&opt.db2.password, "password2", "", "")
	cmd.Flags().StringVar(&opt.db2.version, "ver2", "", "")
	cmd.Flags().StringVar(&opt.queryFile, "queryfile", "", "")
	cmd.Flags().StringVar(&DB, "db", "mysql", "the default database used when connecting to TiDB")
	cmd.Flags().StringVar(&errMode, "errmode", "exit", "the action to do if errors occur when running SQL, exit / print")
	cmd.Flags().BoolVar(&digestMode, "digestmode", false, "SQLs with the same digest only be printed once if it is true")
	return cmd
}

func capturePlanChanges(db1, db2 *tidbHandler, sqls []string, errMode string, digestMode bool) error {
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
			if errMode == "exit" {
				r1, err = runExplain(db1, sql)
				if err != nil {
					return fmt.Errorf("run %v on db1 err=%v", sql, err)
				}
				r2, err = runExplain(db2, sql)
				if err != nil {
					return fmt.Errorf("run %v on db2 err=%v", sql, err)
				}
				p1, err = plan.Parse(db1.opt.version, sql, r1)
				if err != nil {
					return fmt.Errorf("parse %v err=%v", sql, err)
				}
				p2, err = plan.Parse(db2.opt.version, sql, r2)
				if err != nil {
					return fmt.Errorf("parse %v err=%v", sql, err)
				}
			} else if errMode == "print" {
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
