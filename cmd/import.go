package cmd

import (
	"fmt"
	"io/ioutil"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

type importOpt struct {
	db  tidbAccessOptions
	dir string
}

func newImportCmd() *cobra.Command {
	var opt importOpt
	cmd := &cobra.Command{
		Use:   "import",
		Short: "import schemas and statistic information into a TiDB instance",
		Long:  `import schemas and statistic information into a TiDB instance`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("begin to import schemas and statistics information into destination databases")
			db, err := connectDB(opt.db, "mysql")
			if err != nil {
				return fmt.Errorf("connect to DB error: %v", err)
			}
			return importSchemaStats(db, opt.dir)
		},
	}
	cmd.Flags().StringVar(&opt.db.addr, "addr", "127.0.0.1", "address of the target TiDB")
	cmd.Flags().StringVar(&opt.db.port, "port", "4000", "port of the target TiDB")
	cmd.Flags().StringVar(&opt.db.user, "user", "", "user name to access the target TiDB")
	cmd.Flags().StringVar(&opt.db.password, "password", "", "password to access the target TiDB")
	cmd.Flags().StringVar(&opt.dir, "schema-stats-dir", "", "the directory which stores schemas and statistics")
	return cmd
}

func importSchemaStats(db *tidbHandler, dir string) error {
	dbTables, err := parseDBTables(dir)
	if err != nil {
		return fmt.Errorf("parse db and tables from %v error: %v", dir, err)
	}
	for dbName, tables := range dbTables {
		for _, tableName := range tables {
			if err = importSchemas(db, dbName, tableName, dir); err != nil {
				return fmt.Errorf("import schemas error: %v", err)
			}
			if err = importStats(db, dbName, tableName, dir); err != nil {
				return fmt.Errorf("import statistics information error: %v", err)
			}
		}
	}
	return nil
}

func importSchemas(db *tidbHandler, dbName, table, dir string) error {
	schemaPath := schemaPath(dbName, table, dir)
	schemaSQL, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema info from %v error: %v", schemaPath, err)
	}
	if err := db.execute(fmt.Sprintf("create database if not exists `%v`", db),
		fmt.Sprintf("use %v", dbName), string(schemaSQL)); err != nil {
		return err
	}
	fmt.Printf("import schemas from %v successfully\n", schemaPath)
	return nil
}

func importStats(db *tidbHandler, dbName, table, dir string) error {
	statsPath := statsPath(dbName, table, dir)
	mysql.RegisterLocalFile(statsPath)
	fmt.Printf("import schemas from %v successfully\n", statsPath)
	return db.execute(fmt.Sprintf("load stats '%v'", statsPath))
}
