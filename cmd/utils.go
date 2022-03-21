package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tiup/pkg/localdata"
	"github.com/qw4990/plan-change-capturer/instance"
)

func compareVer(ver1, ver2 string) int {
	if ver1 < ver2 {
		return -1
	} else if ver1 == ver2 {
		return 0
	}
	return 1
}

type tidbAccessOptions struct {
	addr       string
	statusPort string
	port       string
	user       string
	password   string
	version    string
}

func (opt *tidbAccessOptions) IntPort() int {
	if opt.port != "" {
		p, err := strconv.Atoi(opt.port)
		if err == nil {
			return p
		}
	}
	return 0
}

func (opt *tidbAccessOptions) IntStatusPort() int {
	if opt.statusPort != "" {
		p, err := strconv.Atoi(opt.statusPort)
		if err == nil {
			return p
		}
	}
	return 0
}

type tidbHandler struct {
	opt tidbAccessOptions
	db  *sql.DB
	p   *localdata.Process
}

func (db *tidbHandler) getDBs() ([]string, error) {
	rows, err := db.db.Query("show databases")
	if err != nil {
		return nil, fmt.Errorf("execute show databases error: %v", err)
	}
	defer rows.Close()
	dbs := make([]string, 0, 8)
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("scan rows err: %v", err)
		}
		if !isSysDB(dbName) {
			dbs = append(dbs, dbName)
		}
	}
	return dbs, nil
}

func (db *tidbHandler) getTableAndViews(dbName string) ([]string, []string, error) {
	_, err := db.db.Exec("use " + dbName)
	if err != nil {
		return nil, nil, fmt.Errorf("switch to DB: %v error: %v", dbName, err)
	}
	rows, err := db.db.Query("show full tables")
	if err != nil {
		return nil, nil, fmt.Errorf("execute show tables error: %v", err)
	}
	defer rows.Close()
	tables := make([]string, 0, 8)
	views := make([]string, 0, 8)
	for rows.Next() {
		var table, tableType string
		if err := rows.Scan(&table, &tableType); err != nil {
			return nil, nil, fmt.Errorf("scan rows error: %v", err)
		}
		switch strings.ToLower(strings.TrimSpace(tableType)) {
		case "view":
			views = append(views, table)
		case "base table":
			tables = append(tables, table)
		default:
			continue
		}
	}
	return tables, views, nil
}

func (db *tidbHandler) getVersion(shortName bool) (string, error) {
	rows, err := db.db.Query("select version()")
	if err != nil {
		return "", fmt.Errorf("execute `select version()` error: %v", err)
	}
	defer rows.Close()
	var ver string
	rows.Next()
	if err := rows.Scan(&ver); err != nil {
		return "", fmt.Errorf("read version error: %v", err)
	}
	if !shortName {
		return ver, nil
	}

	fields := strings.Split(ver, "-")
	return fields[len(fields)-1], nil
}

func (db *tidbHandler) execute(sqls ...string) error {
	for _, sql := range sqls {
		if _, err := db.db.Exec(sql); err != nil {
			return fmt.Errorf("execute `%v` error: %v", sql, err)
		}
	}
	return nil
}

func (db *tidbHandler) stop() {
	if db.p != nil {
		instance.StopTiDB(db.p)
	}
}

func startAndConnectDB(opt tidbAccessOptions, defaultDB string) (*tidbHandler, error) {
	if opt.version == "" {
		return nil, fmt.Errorf("no TiDB version")
	}
	p, port, status := instance.StartTiDB(opt.version, opt.IntPort(), opt.IntStatusPort())
	fmt.Printf("[PCC]: start TiDB ver=%v, port=%v, statusPort=%v \n", opt.version, port, status)

	opt.port = fmt.Sprintf("%v", port)
	opt.statusPort = fmt.Sprintf("%v", status)
	opt.user = "root"
	opt.password = ""
	opt.addr = "127.0.0.1"
	db, err := connectDB(opt, defaultDB)
	if err != nil {
		return nil, err
	}
	db.p = p
	return db, nil
}

func connectDB(opt tidbAccessOptions, defaultDB string) (*tidbHandler, error) {
	defaultDB = strings.TrimSpace(strings.ToLower(defaultDB))
	if defaultDB == "" {
		defaultDB = "mysql"
	}
	dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%v", opt.user, opt.password, opt.addr, opt.port, defaultDB)
	if opt.password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%s)/%v", opt.user, opt.addr, opt.port, defaultDB)
	}
	db, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, fmt.Errorf("connect to database dns:%v, error: %v", dns, err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping DB %v error: %v", dns, err)
	}
	return &tidbHandler{opt, db, nil}, nil
}

func tmpPathDir() string {
	t := time.Now().Format(time.RFC3339)
	t = strings.ReplaceAll(t, ":", "-")
	return filepath.Join(os.TempDir(), "plan-change-capturer", t)
}

var sysDBs = []string{"INFORMATION_SCHEMA", "METRICS_SCHEMA", "PERFORMANCE_SCHEMA", "mysql"}

func isSysDB(db string) bool {
	for _, sysDB := range sysDBs {
		if strings.ToLower(db) == strings.ToLower(sysDB) {
			return true
		}
	}
	return false
}

func schemaPath(db, table, dir string) string {
	return path.Join(dir, fmt.Sprintf("schema-%v-%v.sql", db, table))
}

func statsPath(db, table, dir string) string {
	return path.Join(dir, fmt.Sprintf("stats-%v-%v.json", db, table))
}

func parseDBTables(dir string) (map[string][]string, error) {
	dbTables := make(map[string][]string)
	exists := make(map[string]struct{})
	err := filepath.Walk(dir, func(fpath string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		fname := path.Base(fpath)
		var fields []string
		if strings.HasPrefix(fname, "schema-") && strings.HasSuffix(fname, ".sql") {
			fields = strings.Split(fname[len("schema-"):len(fname)-len(".sql")], "-")
		}
		if strings.HasPrefix(fname, "stats-") && strings.HasSuffix(fname, ".json") {
			fields = strings.Split(fname[len("stats-"):len(fname)-len(".json")], "-")
		}
		if len(fields) == 2 {
			db := fields[0]
			table := fields[1]
			if _, ok := exists[db+"."+table]; ok {
				return nil
			}
			exists[db+"."+table] = struct{}{}
			dbTables[db] = append(dbTables[db], table)
		}
		return nil
	})
	return dbTables, err
}

func stringSliceToMap(strs []string) map[string]struct{} {
	m := make(map[string]struct{}, len(strs))
	for _, str := range strs {
		m[str] = struct{}{}
	}
	return m
}
