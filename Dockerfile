# 这里换成自己的镜像
FROM go-builder:1.17-alpine as builder

ARG APP_NAME
ENV APP_NAME=$APP_NAME
 
WORKDIR $GOPATH/${APP_NAME}/

COPY go.mod $GOPATH/${APP_NAME}/
COPY go.sum $GOPATH/${APP_NAME}/
RUN go env -w CGO_ENABLED="1"
RUN go mod download
COPY . $GOPATH/${APP_NAME}/
RUN go build -o /usr/local/bin/recuffer main.go
# 这里换成自己的镜像
FROM go-runner:1.0
 
ARG APP_NAME
ENV APP_NAME $APP_NAME

WORKDIR /usr/local/bin/
 
COPY --from=builder /usr/local/bin/recuffer /usr/local/bin/

CMD ["/usr/local/bin/recuffer"]
