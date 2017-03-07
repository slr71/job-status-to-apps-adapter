FROM discoenv/golang-base:master

ENV CONF_TEMPLATE=/go/src/github.com/cyverse-de/job-status-to-apps-adapter/jobservices.yml.tmpl
ENV CONF_FILENAME=jobservices.yml
ENV PROGRAM=job-status-to-apps-adapter

COPY . /go/src/github.com/cyverse-de/job-status-to-apps-adapter
RUN go install github.com/cyverse-de/job-status-to-apps-adapter

ARG git_commit=unknown
ARG version="2.9.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
