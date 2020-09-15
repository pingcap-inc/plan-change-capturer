package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newQueryCmd() *cobra.Command {
	var db tidbAccessOptions
	var dstFile string
	var mode string
	cmd := &cobra.Command{
		Use:   "query",
		Short: "export queries from TiDB",
		Long:  `export queries from TiDB`,
		RunE: func(cmd *cobra.Command, args []string) error {
			mode = strings.TrimSpace(strings.ToLower(mode))
			if mode == "statement_summary" {
				if compareVer(db.version, "4.0") == -1 {
					return fmt.Errorf("TiDB:%v doesn't support statement summary", db.version)
				}
			}
			dstFile = strings.TrimSpace(dstFile)
			if dstFile == "" {
				return fmt.Errorf("no path specified")
			}
			dbHandle, err := newDBHandler(db, "information_schema")
			if err != nil {
				return err
			}
			return exportQueriesFromStmtSummary(dbHandle, dstFile)
		}}

	cmd.Flags().StringVar(&db.addr, "addr", "", "")
	cmd.Flags().StringVar(&db.port, "port", "", "")
	cmd.Flags().StringVar(&db.user, "user", "", "")
	cmd.Flags().StringVar(&db.password, "password", "", "")
	cmd.Flags().StringVar(&db.version, "ver", "", "")
	cmd.Flags().StringVar(&dstFile, "path", "", "")
	cmd.Flags().StringVar(&mode, "mode", "statement_summary", "the action to do to export queries from TiDB; only 'statement_summary' now")
	return cmd
}

func exportQueriesFromStmtSummary(db *tidbHandler, dstFile string) error {
	rows, err := db.db.Query("SELECT QUERY_SAMPLE_TEXT FROM information_schema.cluster_statements_summary_history WHERE lower(QUERY_SAMPLE_TEXT) LIKE '*select*'")
	if err != nil {
		return fmt.Errorf("select queries from information_schema.cluster_statements_summary_history error: %v", err)
	}
	defer rows.Close()
	var queries []string
	for rows.Next() {
		var query string
		if err := rows.Scan(&query); err != nil {
			return fmt.Errorf("scan result error: %v", err)
		}
		queries = append(queries, query)
	}

	file, err := os.OpenFile(dstFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := bufio.NewWriter(file)
	for _, q := range queries {
		if _, err := buf.Write([]byte(q + ";\n")); err != nil {
			return err
		}
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	fmt.Printf("export queries from statement_summary into %v successfully\n", dstFile)
	return nil
}
