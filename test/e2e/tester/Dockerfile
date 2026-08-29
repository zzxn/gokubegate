# Built from repo root:
#   CGO_ENABLED=0 go build -o test/e2e/tester/tester ./test/e2e/tester
#   docker build -t gokubegate-e2e/tester:test -f test/e2e/tester/Dockerfile .
FROM scratch
COPY test/e2e/tester/tester /app/tester
ENTRYPOINT ["/app/tester"]
