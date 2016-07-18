FROM jeanblanchard/alpine-glibc
ARG git_commit=unknown
ARG buildenv_git_commit=unknown
ARG version=unknown
COPY job-status-to-apps-adapter /bin/job-status-to-apps-adapter
CMD ["job-status-to-apps-adapter" "--help"]
