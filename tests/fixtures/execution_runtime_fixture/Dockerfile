FROM golang:1.26.10-alpine@sha256:a2012b4a1d1f9f7e9f44e0d39b805a901e856b1dd80b4e6ee4338db547050308 AS build
ARG TARGETARCH
WORKDIR /src
COPY tests/fixtures/execution_runtime_fixture/main.go ./main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/fixture ./main.go

FROM scratch
COPY --from=build /out/fixture /fixture
USER 65532:65532
ENTRYPOINT ["/fixture"]
