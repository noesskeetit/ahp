FROM node:22-bookworm-slim@sha256:c3de60bf2f9dd0ac6370e6117950ff62d6e339527e7472301c9c78a017978392
# The slim image has Node's public roots but no OS CA bundle. Go needs the
# latter for verified HTTPS to model/embedding providers; never skip TLS checks.
RUN node -e "const f=require('fs');f.mkdirSync('/etc/ssl/certs',{recursive:true});f.writeFileSync('/etc/ssl/certs/ca-certificates.crt',require('tls').rootCertificates.join('\n')+'\n')"
WORKDIR /app
COPY ahp as-runner artifact-parser /usr/local/bin/
ENV AHP_ARTIFACT_PARSER=/usr/local/bin/artifact-parser
COPY frontend /app/frontend
COPY human-origin.mjs /app/human-origin.mjs
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/ahp"]
