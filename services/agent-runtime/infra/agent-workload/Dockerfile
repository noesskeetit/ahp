FROM scratch
COPY --chmod=0555 ahp-agent /ahp-agent
USER 65532:65532
EXPOSE 8443
ENTRYPOINT ["/ahp-agent"]
