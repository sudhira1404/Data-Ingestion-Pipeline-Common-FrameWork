FROM docker.target.com/toolshed/base-runtime-connector-jre:17

ADD build/distributions/mdf-api-kelsa-dip.tar /

USER root

CMD ["/mdf-api-kelsa-dip/bin/mdf-api-kelsa-dip"]