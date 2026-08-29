# Built from repo root:
#   CGO_ENABLED=0 go build -o test/e2e/downstream/downstream ./test/e2e/downstream
#   docker build -t gokubegate-e2e/downstream:test -f test/e2e/downstream/Dockerfile .
FROM scratch
COPY test/e2e/downstream/downstream /app/downstream
ENTRYPOINT ["/app/downstream"]
