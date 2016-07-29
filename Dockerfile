FROM golang:1.6-alpine

ARG git_commit=unknown
LABEL org.cyverse.git-ref="$git_commit"

COPY . /go/src/github.com/cyverse-de/job-status-to-apps-adapter
RUN go install github.com/cyverse-de/job-status-to-apps-adapter

ENTRYPOINT ["job-status-to-apps-adapter"]
CMD ["--help"]
