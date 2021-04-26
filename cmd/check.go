package cmd

import (
	"fmt"
	"github.com/qw4990/plan-change-capturer/plan"
	"github.com/spf13/cobra"
	"io/ioutil"
	"strings"
)

type checkOpt struct {
	filepath string
	ver1     string
	ver2     string
}

func newCheckCmd() *cobra.Command {
	var opt checkOpt
	cmd := &cobra.Command{
		Use:   "check",
		Short: "check some plans manually",
		Long:  `check some plans manually`,
		RunE: func(cmd *cobra.Command, args []string) error {
			content, err := ioutil.ReadFile(opt.filepath)
			if err != nil {
				return err
			}
			lines := strings.Split(string(content), "\n")

			var sql, plan1, plan2 string
			beginLine, lineCount := -1, 0
			for i, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				if !isPlanBoundary(line) && lineCount == 0 {
					sql = line
					continue
				}

				if isPlanBoundary(line) {
					lineCount++
					if lineCount == 1 {
						beginLine = i
					} else if lineCount == 3 {
						if plan1 == "" {
							plan1 = strings.Join(lines[beginLine:i+1], "\n")
						} else {
							plan2 = strings.Join(lines[beginLine:i+1], "\n")
							if err := check(sql, opt.ver1, plan1, opt.ver2, plan2); err != nil {
								return err
							}
							plan1 = ""
							plan2 = ""
							sql = ""
						}
						lineCount = 0
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opt.ver1, "ver1", plan.V3, "TiDB version1")
	cmd.Flags().StringVar(&opt.ver2, "ver2", plan.V4, "TiDB version2")
	cmd.Flags().StringVar(&opt.filepath, "path", "", "File Path")
	return cmd
}

func check(sql, v1, p1, v2, p2 string) error {
	plan1, err := plan.ParseText(sql, p1, v1)
	if err != nil {
		return err
	}
	plan2, err := plan.ParseText(sql, p2, v2)
	if err != nil {
		return err
	}
	reas, same := plan.Compare(plan1, plan2, true)
	if !same {
		fmt.Println("==============================================================================")
		fmt.Println("SQL: ", sql)
		fmt.Println("Plan1: ")
		fmt.Println(p1)
		fmt.Println("Plan2: ")
		fmt.Println(p2)
		fmt.Println(" Reason: ", reas)
	}
	return nil
}

func isPlanBoundary(line string) bool {
	for _, b := range line {
		if b != '+' && b != '-' && b != ' ' {
			return false
		}
	}
	return true
}
