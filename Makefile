KIND_VERSION ?= v0.27.0
KIND := .cache/kind/kind

.PHONY: e2e-bootstrap
e2e-bootstrap: ## install kind into .cache (go install; falls back to binary download)
	@mkdir -p .cache/kind
	@if [ ! -x "$(KIND)" ]; then \
		( export GOPROXY="$${GOPROXY:-https://goproxy.cn,direct}"; \
		  echo "installing kind $(KIND_VERSION) via go install ..."; \
		  go install sigs.k8s.io/kind@$(KIND_VERSION) && cp "$$(go env GOPATH)/bin/kind" "$(KIND)" ) \
		|| { echo "go install failed; trying binary download ..."; \
		     curl -fL --retry 3 --connect-timeout 10 -o "$(KIND).tmp" \
		       "https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-linux-amd64" && \
		     chmod +x "$(KIND).tmp" && mv "$(KIND).tmp" "$(KIND)"; }; \
	fi
	@echo "kind ready: $(KIND)"

.PHONY: e2e
e2e: e2e-bootstrap ## run real-cluster integration tests (kind + Docker)
	@PATH="$(CURDIR)/.cache/kind:$$PATH" go test -tags e2e -timeout 30m -v $(ARGS) ./test/e2e/harness/...

.PHONY: e2e-clean
e2e-clean: ## delete the e2e kind cluster and local cache
	@if [ -x "$(KIND)" ]; then "$(KIND)" delete cluster --name gokubegate-e2e 2>/dev/null || true; fi
	@rm -rf .cache/e2e test/e2e/downstream/downstream test/e2e/tester/tester
	@echo "e2e artifacts cleaned"
