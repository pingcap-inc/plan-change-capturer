package cmd

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

type tidbAccessOptions struct {
	addr       string
	statusPort string
	port       string
	user       string
	password   string
	version    string
}

type tidbHandler struct {
	opt tidbAccessOptions
	db  *sql.DB
}

func newDBHandler(opt tidbAccessOptions) (*tidbHandler, error) {
	dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/mysql", opt.user, opt.password, opt.addr, opt.port)
	if opt.password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%s)/mysql", opt.user, opt.addr, opt.port)
	}
	db, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, fmt.Errorf("connect to database dns:%v, error: %v", dns, err)
	}
	return &tidbHandler{opt, db}, nil
}

type transportOptions struct {
	src tidbAccessOptions
	dst tidbAccessOptions
	dir string
	dbs []string
}

func newTransportCmd() *cobra.Command {
	var opt transportOptions
	cmd := &cobra.Command{
		Use:   "transport",
		Short: "import/export/transport schemas and statistic information",
		Long:  `import/export/transport schemas and statistic information`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opt.dir == "" {
				opt.dir = tmpPathDir()
			}
			if err := os.MkdirAll(opt.dir, 0776); err != nil {
				return fmt.Errorf("create destination directory error: %v", err)
			}
			if opt.src.addr != "" {
				fmt.Println("begin to export schemas and statistics information from source databases")
				src, err := newDBHandler(opt.src)
				if err != nil {
					return fmt.Errorf("create source DB handler error: %v", err)
				}
				if err = exportSchemas(src, opt.dbs, opt.dir); err != nil {
					return fmt.Errorf("export schemas error: %v", err)
				}
				if err = exportStats(src, opt.dbs, opt.dir); err != nil {
					return fmt.Errorf("export statistics information error: %v", err)
				}
				fmt.Println("export schemas and statistics information from source databases successfully")
			}
			if opt.dst.addr != "" {
				fmt.Println("begin to import schemas and statistics information into destination databases")
				dst, err := newDBHandler(opt.dst)
				if err != nil {
					return fmt.Errorf("create destination DB handler error: %v", err)
				}
				if err = importSchemas(dst, opt.dbs, opt.dir); err != nil {
					return fmt.Errorf("import schemas error: %v", err)
				}
				if err = importStats(dst, opt.dbs, opt.dir); err != nil {
					return fmt.Errorf("import statistics information error: %v", err)
				}
				fmt.Println("import schemas and statistics information into destination databases successfully")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opt.src.addr, "srcaddr", "", "")
	cmd.Flags().StringVar(&opt.src.port, "srcport", "4000", "")
	cmd.Flags().StringVar(&opt.src.statusPort, "srcstatusport", "10080", "")
	cmd.Flags().StringVar(&opt.src.user, "srcuser", "", "")
	cmd.Flags().StringVar(&opt.src.password, "srcpassword", "", "")
	cmd.Flags().StringVar(&opt.dst.addr, "dstaddr", "", "")
	cmd.Flags().StringVar(&opt.dst.port, "dstport", "4000", "")
	cmd.Flags().StringVar(&opt.dst.statusPort, "dststatusport", "10080", "")
	cmd.Flags().StringVar(&opt.dst.user, "dstuser", "", "")
	cmd.Flags().StringVar(&opt.dst.password, "dstpassword", "", "")
	cmd.Flags().StringVar(&opt.dir, "dir", "", "")
	cmd.Flags().StringSliceVar(&opt.dbs, "dbs", nil, "")
	return cmd
}

func exportSchemas(h *tidbHandler, dbs []string, dir string) error {
	for _, db := range dbs {
		if err := exportDBSchemas(h, db, dir); err != nil {
			return fmt.Errorf("export DB: %v schemas to %v error: %v", db, dir, err)
		}
	}
	return nil
}

func exportDBSchemas(h *tidbHandler, db, dir string) error {
	tables, err := getTables(h, db)
	if err != nil {
		return fmt.Errorf("get DB: %v table information error: %v", db, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("schema-%v.sql", db))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := bufio.NewWriter(file)
	for _, t := range tables {
		showSQL := fmt.Sprintf("show create table `%v`.`%v`", db, t)
		rows, err := h.db.Query(showSQL)
		if err != nil {
			return fmt.Errorf("exec SQL: %v error: %v", showSQL, err)
		}
		rows.Next()
		var table, createSQL string
		if err := rows.Scan(&table, &createSQL); err != nil {
			rows.Close()
			return fmt.Errorf("scan rows error: %v", err)
		}
		if !strings.HasSuffix(createSQL, ";") {
			createSQL += ";"
		}
		if _, err := buf.WriteString(createSQL + "\n\n"); err != nil {
			rows.Close()
			return fmt.Errorf("write buffer error: %v", err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	fmt.Printf("export %s:%s/%s schemas into %v\n", h.opt.addr, h.opt.port, db, path)
	return buf.Flush()
}

func exportStats(h *tidbHandler, dbs []string, dir string) error {
	for _, db := range dbs {
		tables, err := getTables(h, db)
		if err != nil {
			return fmt.Errorf("get DB: %v table information error: %v", db, err)
		}
		for _, t := range tables {
			if err := exportTableStats(h, db, t, dir); err != nil {
				return fmt.Errorf("export DB: %v table: %v statistics to %v error: %v", db, t, dir, err)
			}
		}
	}
	return nil
}

func exportTableStats(h *tidbHandler, db, table, dir string) error {
	addr := fmt.Sprintf("http://%v:%v/stats/dump/%v/%v", h.opt.addr, h.opt.statusPort, db, table)
	resp, err := http.Get(addr)
	if err != nil {
		return fmt.Errorf("request URL: %v error: %v", addr, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("stats-%v-%v.json", db, table))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := bufio.NewWriter(file)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		return err
	}
	if err := buf.Flush(); err != nil {
		return err
	}
	fmt.Printf("export %v:%v/%v.%v stats into %v\n", h.opt.addr, h.opt.port, db, table, path)
	return nil
}

func importSchemas(h *tidbHandler, dbs []string, dir string) error {
	for _, db := range dbs {
		if _, err := h.db.Exec(fmt.Sprintf("create database if not exists `%v`", db)); err != nil {
			return fmt.Errorf("create DB: %v error: %v", db, err)
		}
		if _, err := h.db.Exec("use " + db); err != nil {
			return fmt.Errorf("switch to DB: %v error: %v", db, err)
		}
		path := filepath.Join(dir, fmt.Sprintf("schema-%v.sql", db))
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		sqls := strings.Split(string(content), ";")
		for _, sql := range sqls {
			sql = strings.TrimSpace(sql)
			if _, err := h.db.Exec(sql); err != nil {
				return fmt.Errorf("execute SQL: %v from %v error: %v", sql, path, err)
			}
		}
		fmt.Printf("import schemas from %v into %v:%v/%v\n", path, h.opt.addr, h.opt.port, db)
	}
	return nil
}

func importStats(h *tidbHandler, dbs []string, dir string) error {
	for _, db := range dbs {
		files, err := filepath.Glob(filepath.Join(dir, "stats-"+db+"-*[.]json"))
		if err != nil {
			return err
		}
		for _, fpath := range files {
			mysql.RegisterLocalFile(fpath)
			if _, err := h.db.Exec(fmt.Sprintf("load stats '%v'", fpath)); err != nil {
				return err
			}
			fmt.Printf("import stats from %v into %v:%v/%v\n", fpath, h.opt.addr, h.opt.port, db)
		}
	}
	return nil
}

func getTables(h *tidbHandler, db string) ([]string, error) {
	_, err := h.db.Exec("use " + db)
	if err != nil {
		return nil, fmt.Errorf("switch to DB: %v error: %v", db, err)
	}
	rows, err := h.db.Query("show tables")
	if err != nil {
		return nil, fmt.Errorf("execute show tables error: %v", err)
	}
	defer rows.Close()
	tables := make([]string, 0, 8)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("scan rows error: %v", err)
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func tmpPathDir() string {
	t := time.Now().Format(time.RFC3339)
	t = strings.ReplaceAll(t, ":", "-")
	return filepath.Join(os.TempDir(), "plan-change-capturer", t)
}
