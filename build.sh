function compile()
{
  echo "build for $1-$2"
  GITHASH=$(git rev-parse HEAD)
  GO111MODULE=on CGO_ENABLED=0 GOOS=$1 GOARCH=$2 go build -o bin/plan-change-capturer main.go
  cd bin
  tar --uname "" --gname "" --uid 0 --gid 0 -czf plan-change-capturer-$1-$2.tar.gz plan-change-capturer
  cd ..
}

export GO111MODULE=on
export GOPROXY=https://proxy.golang.org

OS=darwin
ARCH=amd64
compile $OS $ARCH

OS=darwin
ARCH=arm64
compile $OS $ARCH

OS=linux
ARCH=amd64
compile $OS $ARCH

OS=linux
ARCH=arm64
compile $OS $ARCH