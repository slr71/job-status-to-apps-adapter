FROM golang:1.7-alpine

ARG git_commit=unknown
ARG version="2.9.0"

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"

COPY . /go/src/github.com/cyverse-de/job-status-to-apps-adapter
RUN go install github.com/cyverse-de/job-status-to-apps-adapter

ENTRYPOINT ["job-status-to-apps-adapter"]
CMD ["--help"]
