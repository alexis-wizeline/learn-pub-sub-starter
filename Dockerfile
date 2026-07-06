FROM rabbitmq:3.11.23-management
RUN rabbitmq-plugins enable rabbitmq_stomp
