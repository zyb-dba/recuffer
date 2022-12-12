#!/usr/bin/env bash

RECUFFER_ADMIN="${BASH_SOURCE-$0}"
RECUFFER_ADMIN="$(dirname "${RECUFFER_ADMIN}")"
RECUFFER_ADMIN_DIR="$(cd "${RECUFFER_ADMIN}"; pwd)"

RECUFFER_BIN_DIR=$RECUFFER_ADMIN_DIR/../bin
RECUFFER_LOG_DIR=$RECUFFER_ADMIN_DIR/../log
RECUFFER_CONF_DIR=$RECUFFER_ADMIN_DIR/../config

RECUFFER_PROXY_BIN=$RECUFFER_BIN_DIR/recuffer
RECUFFER_PROXY_PID_FILE=$RECUFFER_BIN_DIR/recuffer.pid

RECUFFER_PROXY_LOG_FILE=$RECUFFER_LOG_DIR/recuffer.log
RECUFFER_PROXY_DAEMON_FILE=$RECUFFER_LOG_DIR/recuffer.out

RECUFFER_PROXY_CONF_FILE=$RECUFFER_CONF_DIR/recuffer.toml

echo $RECUFFER_PROXY_CONF_FILE

if [ ! -d $RECUFFER_LOG_DIR ]; then
    mkdir -p $RECUFFER_LOG_DIR
fi


case $1 in
start)
    echo  "starting recuffer ... "
    if [ -f "$RECUFFER_PROXY_PID_FILE" ]; then
      if kill -0 `cat "$RECUFFER_PROXY_PID_FILE"` > /dev/null 2>&1; then
         echo $command already running as process `cat "$RECUFFER_PROXY_PID_FILE"`.
         exit 0
      fi
    fi
    nohup "$RECUFFER_PROXY_BIN" "--config=${RECUFFER_PROXY_CONF_FILE}" "--proxy_type=rediscluster" \
    "--log=$RECUFFER_PROXY_LOG_FILE" "--log-level=WARNING" "--ncpu=8" "--max-ncpu=16" "--pidfile=$RECUFFER_PROXY_PID_FILE" > "$RECUFFER_PROXY_DAEMON_FILE" 2>&1 < /dev/null &
    ;;
start-foreground)
    $RECUFFER_PROXY_BIN "--config=${RECUFFER_PROXY_CONF_FILE}" "--proxy_type=rediscluster" \
    "--log-level=WARNING" "--pidfile=$RECUFFER_PROXY_PID_FILE"
    ;;
stop)
    echo "stopping recuffer ... "
    if [ ! -f "$RECUFFER_PROXY_PID_FILE" ]
    then
      echo "no recuffer to stop (could not find file $RECUFFER_PROXY_PID_FILE)"
    else
      kill -2 $(cat "$RECUFFER_PROXY_PID_FILE")
      echo STOPPED
    fi
    exit 0
    ;;
stop-forced)
    echo "stopping recuffer ... "
    if [ ! -f "$RECUFFER_PROXY_PID_FILE" ]
    then
      echo "no recuffer to stop (could not find file $RECUFFER_PROXY_PID_FILE)"
    else
      kill -9 $(cat "$RECUFFER_PROXY_PID_FILE")
      rm "$RECUFFER_PROXY_PID_FILE"
      echo STOPPED
    fi
    exit 0
    ;;
restart)
    shift
    "$0" stop
    sleep 1
    "$0" start
    ;;
*)
    echo "Usage: $0 {start|start-foreground|stop|stop-forced|restart}" >&2

esac
