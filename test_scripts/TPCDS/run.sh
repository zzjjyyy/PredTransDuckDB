#!/bin/bash
 
trap "endDuckDB; exit;" SIGINT

# 检查进程是否运行的函数
is_process_running() {
    pgrep $PROCESS_NAME >/dev/null 2>&1
    return $?
}

endDuckDB() {
    echo "endDuckDB"
    while is_process_running; do
        kill $child_pid 2>/dev/null
        sleep 1s
    done
}

# 进程名称
PROCESS_NAME="TPCDS"
 
# 启动进程的函数
start_process() {
    ./TPCDS $1 2>&1 &
    child_pid=$!
    for ((k = 0; k < 1000; k++)); do
        pgrep $PROCESS_NAME >/dev/null 2>&1
        if [ $? -ne 0 ]
        then
            return
        fi
        if (($k % 60 == 0))
        then
            echo $k
        fi
        sleep 1s
    done
    endDuckDB
}
 
# 主循环
for i in {1..99}; do
    for ((j = 0; j < 1; j++)); do
        start_process $i
    done
done
