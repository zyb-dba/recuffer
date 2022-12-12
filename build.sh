#!/bin/bash

RECUFFER_SRC_DIR=$(cd `dirname $0`;pwd)
echo $RECUFFER_SRC_DIR

RECUFFER_BIN_DIR=$RECUFFER_SRC_DIR/bin
RECUFFER_VER_FILE=$RECUFFER_SRC_DIR/pkg/utils/version.go

GO_BUILD_CMD="go build"

declare -A BUILD_LIST
BUILD_LIST=(
    [recuffer]='main.go'
)

if [ ! -d "$RECUFFER_BIN_DIR" ]; then
    mkdir $RECUFFER_BIN_DIR
fi
#if [ ! -f "$RECUFFER_BIN_DIR/version" ]; then
#    `cp $RECUFFER_SRC_DIR/version $RECUFFER_BIN_DIR`
#fi
# update codis version file
#if [ -f "$RECUFFER_VER_FILE" ]; then
#    rm $RECUFFER_VER_FILE && /home/homework/dev/src/github.com/CodisLabs/codis/version
#    #$RECUFFER_SRC_DIR/version
#fi

# compile bins
for bin_name in ${!BUILD_LIST[*]}; do
    echo "TARGET: $bin_name"
    $($GO_BUILD_CMD -o $RECUFFER_BIN_DIR/$bin_name ${BUILD_LIST[$bin_name]})
    if [ $? == 0 ]; then
        echo "BUILD SUCCESS"
    else
        echo "BUILD FAILED"
    fi
done
