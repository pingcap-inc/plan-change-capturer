#!/bin/sh

# curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/qw4990/plan-change-capturer/master/install.sh | sh

repo='https://github.com/qw4990/plan-change-capturer'

case $(uname -s) in
    Linux|linux) os=linux ;;
    Darwin|darwin) os=darwin ;;
    *) os= ;;
esac

if [ -z "$os" ]; then
    echo "OS $(uname -s) not supported." >&2
    exit 1
fi

case $(uname -m) in
    amd64|x86_64) arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) arch= ;;
esac

if [ -z "$arch" ]; then
    echo "Architecture  $(uname -m) not supported." >&2
    exit 1
fi

PCC_HOME=$HOME/.plan-change-capturer
bin_dir=PCC_HOME/bin
mkdir -p "$bin_dir"

install_binary() {
    echo "Try to dowmload archive from https://raw.githubusercontent.com/qw4990/plan-change-capturer/master/bin/plan-change-capturer-$os-$arch.tar.gz"
    curl "https://raw.githubusercontent.com/qw4990/plan-change-capturer/master/bin/plan-change-capturer-$os-$arch.tar.gz" -o "/tmp/plan-change-capturer-$os-$arch.tar.gz" || return 1
    tar -zxf "/tmp/plan-change-capturer-$os-$arch.tar.gz" -C "$bin_dir" || return 1
    rm "/tmp/plan-change-capturer-$os-$arch.tar.gz"
    return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract TiDB plan-change-capturer archive."
    exit 1
fi

chmod 755 "$bin_dir/plan-change-capturer"

bold=$(tput bold 2>/dev/null)
sgr0=$(tput sgr0 2>/dev/null)

# Refrence: https://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux-unix
shell=$(echo $SHELL | awk 'BEGIN {FS="/";} { print $NF }')
echo "Detected shell: ${bold}$shell${sgr0}"
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE=${HOME}/.${shell}_profile
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE=${HOME}/.${shell}_login
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE=${HOME}/.${shell}rc
else
    PROFILE=${HOME}/.profile
fi
echo "Shell profile:  ${bold}$PROFILE${sgr0}"

case :$PATH: in
    *:$bin_dir:*) : "PATH already contains $bin_dir" ;;
    *) printf '\nexport PATH=%s:$PATH\n' "$bin_dir" >> "$PROFILE"
        echo "$PROFILE has been modified to add TiDB plan-change-capturer to PATH"
        echo "open a new terminal or ${bold}source ${PROFILE}${sgr0} to use it"
        ;;
esac

echo "Installed path: ${bold}$bin_dir/plan-change-capturer${sgr0}"
echo "==============================================="
echo "Have a try:     ${bold}plan-change-capturer --help${sgr0}"
echo "==============================================="