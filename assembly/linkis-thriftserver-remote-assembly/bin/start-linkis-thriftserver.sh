#!/bin/bash

# get log directory
cd `dirname $0`
cd ..
INSTALL_HOME=`pwd`

# set LINKIS_THRIFTSERVER_HOME
if [ "$LINKIS_THRIFTSERVER_HOME" = "" ]; then
  export LINKIS_THRIFTSERVER_HOME=$INSTALL_HOME
fi

# set LINKIS_THRIFTSERVER_CONF_DIR
if [ "$LINKIS_THRIFTSERVER_CONF_DIR" = "" ]; then
  export LINKIS_THRIFTSERVER_CONF_DIR=$LINKIS_THRIFTSERVER_HOME/conf
fi

SERVER_SUFFIX="linkis-thriftserver-remote-server"
## set log
if [ "$LINKIS_THRIFTSERVER_LOG_DIR" = "" ]; then
  export LINKIS_THRIFTSERVER_LOG_DIR="$LINKIS_THRIFTSERVER_HOME/logs"
fi
export SERVER_LOG_PATH=$LINKIS_THRIFTSERVER_LOG_DIR
if [ ! -w "$SERVER_LOG_PATH" ] ; then
  mkdir -p "$SERVER_LOG_PATH"
fi

if test -z "$SERVER_HEAP_SIZE"
then
  export SERVER_HEAP_SIZE="1G"
fi

DEBUG_PORT=
if [ "$DEBUG_PORT" ];
then
   export DEBUG_CMD="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT"
fi

if test -z "$SERVER_JAVA_OPTS"
then
  export SERVER_JAVA_OPTS="-DserviceName=$SERVER_SUFFIX -Xmx$SERVER_HEAP_SIZE -XX:+UseG1GC -Xloggc:$SERVER_LOG_PATH/$SERVER_SUFFIX.gc $DEBUG_CMD"
fi

export SERVER_CLASS=org.apache.linkis.thriftserver.LinkisThriftServerRemoteServer

## conf dir
export SERVER_CONF_PATH=$LINKIS_THRIFTSERVER_CONF_DIR

## server lib
export SERVER_LIB=$LINKIS_THRIFTSERVER_HOME/lib


if [ ! -r "$SERVER_LIB" ] ; then
    echo "server lib not exists $SERVER_LIB"
    exit 1
fi

export SERVER_PID=$LINKIS_THRIFTSERVER_HOME/pid/${SERVER_SUFFIX}.pid
## set class path
export SERVER_CLASS_PATH=$SERVER_CONF_PATH:$SERVER_LIB/*

nohup java $SERVER_JAVA_OPTS -cp $SERVER_CLASS_PATH $SERVER_CLASS 2>&1 > $SERVER_LOG_PATH/$SERVER_SUFFIX.out &
pid=$!

sleep 2
if [[ -z "${pid}" ]]; then
    echo "server $SERVER_SUFFIX start failed!"
    exit 1
else
    echo "server $SERVER_SUFFIX start succeeded!"
    echo $pid > $SERVER_PID
    sleep 1
fi
exit 1
