FROM fluent/fluentd:v1.12.0-debian-1.0

USER root

RUN fluent-gem install fluent-plugin-kafka

COPY ./conf/fluentd.conf /fluentd/etc/fluentd.conf

EXPOSE 5050

CMD ["fluentd", "-c", "/fluentd/etc/fluentd.conf"]
