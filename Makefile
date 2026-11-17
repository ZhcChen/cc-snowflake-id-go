# 检查与测试在远端开发主机运行（本机不构建、不测试），见 scripts/dev/remote.sh。
.PHONY: test bench tidy

test: ## 格式、tidy、vet、单元、竞态（及集成）测试
	scripts/dev/remote.sh test

bench: ## 基准测试
	scripts/dev/remote.sh bench

tidy: ## 远端 go mod tidy 并取回 go.mod、go.sum
	scripts/dev/remote.sh tidy
