package instance

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/tiup/pkg/environment"
	"github.com/pingcap/tiup/pkg/exec"
	"github.com/pingcap/tiup/pkg/localdata"
	"github.com/pingcap/tiup/pkg/repository"
	"github.com/pingcap/tiup/pkg/utils"
)

func init() {
	env, err := environment.InitEnv(repository.Options{})
	if err != nil {
		panic(fmt.Errorf("init env error: %v", err))
	}
	environment.SetGlobalEnv(env)
}
func base62Tag() string {
	const base = 62
	const sets = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 0)
	num := time.Now().UnixNano() / int64(time.Millisecond)
	for num > 0 {
		r := math.Mod(float64(num), float64(base))
		num /= base
		b = append([]byte{sets[int(r)]}, b...)
	}
	return string(b)
}

func StartTiDB(ver string) (p *localdata.Process, port, statusPort int) {
	env := environment.GlobalEnv()
	component, version := environment.ParseCompVersion(fmt.Sprintf("tidb:%v", ver))
	if !env.IsSupportedComponent(component) {
		panic(fmt.Errorf("component `%s` does not support", component))
	}

	var tag string
	instanceDir := os.Getenv(localdata.EnvNameInstanceDataDir)
	if instanceDir == "" {
		if tag == "" {
			tag = base62Tag()
		}
		instanceDir = env.LocalPath(localdata.DataParentDir, tag)
	}

	port = utils.MustGetFreePort("127.0.0.1", 4000)
	statusPort = utils.MustGetFreePort("127.0.0.1", 10080)
	args := []string{fmt.Sprintf("-P=%v", port), fmt.Sprintf("-status=%v", statusPort)}
	c, err := exec.PrepareCommand(context.Background(), "tidb", version, "", tag, instanceDir, "", args, env, true)
	if err != nil {
		panic(err)
	}

	p = &localdata.Process{
		Component:   component,
		CreatedTime: time.Now().Format(time.RFC3339),
		Exec:        c.Args[0],
		Args:        args,
		Dir:         instanceDir,
		Env:         c.Env,
		Cmd:         c,
	}
	fmt.Printf("Starting component `%s`: %s\n", component, strings.Join(append([]string{p.Exec}, p.Args...), " "))
	err = p.Cmd.Start()
	if err != nil {
		panic(err)
	}
	if p.Cmd.Process != nil {
		p.Pid = p.Cmd.Process.Pid
	}
	fmt.Printf("Start tidb:%v -P=%v -status=%v successfully\n", ver, port, statusPort)
	return
}

func StopTiDB(p *localdata.Process) {
	if err := syscall.Kill(p.Pid, syscall.SIGKILL); err != nil {
		panic(err)
	}
}
