#!/usr/bin/env bash
# 在远端开发主机上运行本库的检查与测试（本机不构建、不测试）。
#
# 用法：scripts/dev/remote.sh test|bench|tidy
#   test  格式、go mod tidy -diff、vet、单元测试、竞态测试（NEEDS_PG=1 时另跑 PostgreSQL 集成测试）
#   bench 基准测试
#   tidy  在远端执行 go mod tidy，并把 go.mod、go.sum 取回本机
#
# 做法：把当前工作区（git 跟踪 + 未忽略的新文件）打包同步到远端 <REMOTE_ROOT>/<分支键>/src，
# 用一次性 Go 容器执行（只接入共享网络，不发布端口，结束即删除）；同一分支串行执行（flock）。
# 缓存卷 <LIB>-gocache、<LIB>-gomod 跨分支复用。资源上限：Go 容器 4 CPU / 4 GiB，PostgreSQL 1 CPU / 1 GiB。
set -euo pipefail

# ---- 本库配置 ----
LIB=cc-snowflake-id-go
NEEDS_PG=1
# ------------------

HOST=${REMOTE_HOST:-qfy-test2}
REMOTE_ROOT=/opt/qfy-dev/$LIB
NETWORK=dev-shared-network
# 测试镜像：固定摘要的 golang alpine 加 gcc、musl-dev（竞态测试需要 cgo）。远端没有时自动构建一次，两个库共用。
BASE_IMAGE=golang:1.26.11-alpine@sha256:ae52b5bc6ed045a9ed26ab03dc19327a802a9cc9aa8b27ed57f5c6b60579b0bb
GO_IMAGE=cc-go-test:1.26.11-ae52b5bc
PG_IMAGE=postgres:18-alpine@sha256:6c538e7206ea40ff740ef27883529390a690b6ead6ba96b44c67a9f7c638e8fd
GOPROXY_URL=https://goproxy.cn,direct
LABEL=qfy.dev.project=$LIB

mode=${1:-test}
case $mode in test | bench | tidy) ;; *) echo "用法：$0 test|bench|tidy" >&2; exit 2 ;; esac

cd "$(git rev-parse --show-toplevel)"
branch=$(git rev-parse --abbrev-ref HEAD)
key="$(printf '%s' "$branch" | tr -c 'a-zA-Z0-9' '-' | cut -c1-40)-$(printf '%s' "$branch" | shasum | cut -c1-8)"
dir=$REMOTE_ROOT/$key
name=$LIB-$key

echo "==> 同步 $branch 到 $HOST:$dir/src"
git ls-files -co --exclude-standard -z | COPYFILE_DISABLE=1 tar --no-xattrs --no-mac-metadata --null -T - -czf - |
	ssh "$HOST" "set -e; mkdir -p '$dir'; rm -rf '$dir/src.new'; mkdir '$dir/src.new'; tar -xzf - -C '$dir/src.new'; rm -rf '$dir/src'; mv '$dir/src.new' '$dir/src'"

case $mode in
test)
	steps='test -z "$(gofmt -l .)" || { echo "以下文件未格式化："; gofmt -l .; exit 1; }
go mod tidy -diff
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...'
	;;
bench) steps='go test -run "^$" -bench . -benchmem ./...' ;;
tidy) steps='go mod tidy' ;;
esac

pg_setup=""
pg_cleanup=""
pg_env=""
if [ "$NEEDS_PG" = 1 ] && [ "$mode" = test ]; then
	# 一次性 PostgreSQL：随机密码只存在于本次命令中，结束即删除容器。
	pg_setup="pw=\$(head -c 18 /dev/urandom | base64 | tr -d '/+='); \
docker run -d --rm --name '$name-pg' --network '$NETWORK' --label '$LABEL' --cpus 1 --memory 1g --pids-limit 512 --log-driver local -e POSTGRES_PASSWORD=\"\$pw\" '$PG_IMAGE' >/dev/null; \
for i in \$(seq 1 60); do docker exec '$name-pg' pg_isready -U postgres -q && break; sleep 1; done;"
	pg_cleanup="docker rm -f '$name-pg' >/dev/null 2>&1 || true"
	pg_env="-e IDGEN_TEST_DATABASE_URL=\"postgres://postgres:\$pw@$name-pg:5432/postgres?sslmode=disable\""
	steps="$steps
go test -count=1 -tags=integration ./lease"
fi

echo "==> 准备测试镜像 $GO_IMAGE"
printf 'FROM %s\nRUN sed -i "s#https\\?://dl-cdn.alpinelinux.org#https://mirrors.aliyun.com#" /etc/apk/repositories \\\n && apk add --no-cache gcc musl-dev\n' "$BASE_IMAGE" |
	ssh "$HOST" "docker image inspect '$GO_IMAGE' >/dev/null 2>&1 || docker build -q --label qfy.dev.project=cc-libs -t '$GO_IMAGE' - >/dev/null"

echo "==> 远端执行：$mode"
ssh "$HOST" "set -e; exec 9>'$dir.lock'; flock 9; \
trap \"$pg_cleanup\" EXIT; $pg_setup \
docker run --rm --name '$name-go' --network '$NETWORK' --label '$LABEL' \
--cpus 4 --memory 4g --pids-limit 1024 --log-driver local \
-v '$dir/src:/src' -v '$LIB-gocache:/root/.cache' -v '$LIB-gomod:/go/pkg/mod' -w /src \
-e GOPROXY='$GOPROXY_URL' -e CGO_ENABLED=1 $pg_env '$GO_IMAGE' sh -ec '$(printf '%s' "$steps" | sed "s/'/'\\\\''/g")'"

if [ "$mode" = tidy ]; then
	echo "==> 取回 go.mod、go.sum"
	ssh "$HOST" "cat '$dir/src/go.mod'" >go.mod
	if ssh "$HOST" "test -f '$dir/src/go.sum'"; then ssh "$HOST" "cat '$dir/src/go.sum'" >go.sum; fi
	git status --short go.mod go.sum
fi
echo "==> 完成"
