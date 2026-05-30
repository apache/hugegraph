#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# TODO: consider reuse it with server-dist module (almost same as it)
function command_available() {
    local cmd=$1
    if [ $(command -v $cmd >/dev/null 2>&1) ]; then
        return 1
    else
        return 0
    fi
}

# read a property from .properties file
function read_property() {
    # file path
    file_name=$1
    # replace "." to "\."
    property_name=$(echo $2 | sed 's/\./\\\./g')
    cat $file_name | sed -n -e "s/^[ ]*//g;/^#/d;s/^$property_name=//p" | tail -1
}

function write_property() {
    local file=$1
    local key=$2
    local value=$3

    local os=$(uname)
    case $os in
        # Note: in mac os should use sed -i '' "xxx" to replace string,
        # otherwise prompt 'command c expects \ followed by text'.
        # See http://www.cnblogs.com/greedy-day/p/5952899.html
        Darwin) sed -i '' "s!$key=.*!$key=$value!g" "$file" ;;
        *) sed -i "s!$key=.*!$key=$value!g" "$file" ;;
    esac
}

function parse_yaml() {
    local file=$1
    local version=$2
    local module=$3

    cat $file | tr -d '\n {}'| awk -F',+|:' '''{
        pre="";
        for(i=1; i<=NF; ) {
            if(match($i, /version/)) {
                pre=$i;
                i+=1
            } else {
                result[pre"-"$i] = $(i+1);
                i+=2
            }
        }
    } END {for(e in result) {print e": "result[e]}}''' \
    | grep "$version-$module" | awk -F':' '{print $2}' | tr -d ' ' && echo
}

function process_num() {
    num=`ps -ef | grep $1 | grep -v grep | wc -l`
    return $num
}

function process_id() {
    pid=`ps -ef | grep $1 | grep -v grep | awk '{print $2}'`
    return $pid
}

# check the port of rest server is occupied
function check_port() {
    local port=$(echo "$1" | sed 's|.*:||' | sed 's|/.*||')
    if ! command_available "lsof"; then
        echo "Required lsof but it is unavailable"
        exit 1
    fi
    lsof -i :$port >/dev/null
    if [ $? -eq 0 ]; then
        echo "The port $port has already been used"
        exit 1
    fi
}

function crontab_append() {
    local job="$1"
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    fi
    (crontab -l ; echo "$job") | crontab -
}

function crontab_remove() {
    local job="$1"
    # check exist before remove
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 1 ]; then
        return 0
    fi

    crontab -l | grep -Fv "$job"  | crontab -

    # Check exist after remove
    crontab -l | grep -F "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    else
        return 0
    fi
}

# wait_for_startup friendly_name host port timeout_s
function wait_for_startup() {
    local pid="$1"
    local server_name="$2"
    local server_url="$3"
    local timeout_s="$4"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    local status

    echo -n "Connecting to $server_name ($server_url)"
    while [ $now_s -le $stop_s ]; do
        echo -n .
        process_status "$server_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "Starting $server_name failed"
            return 1
        fi

        status=`curl -o /dev/null -s -k -w %{http_code} $server_url`
        if [[ $status -eq 200 || $status -eq 401 ]]; then
            echo "OK"
            echo "Started [pid $pid]"
            return 0
        fi
        sleep 2
        now_s=`date '+%s'`
    done

    echo "The operation timed out when attempting to connect to $server_url" >&2
    return 1
}

function free_memory() {
    local free=""
    local os=`uname`
    if [ "$os" == "Linux" ]; then
        local mem_free=`cat /proc/meminfo | grep -w "MemFree" | awk '{print $2}'`
        local mem_buffer=`cat /proc/meminfo | grep -w "Buffers" | awk '{print $2}'`
        local mem_cached=`cat /proc/meminfo | grep -w "Cached" | awk '{print $2}'`
        if [[ "$mem_free" == "" || "$mem_buffer" == "" || "$mem_cached" == "" ]]; then
            echo "Failed to get free memory"
            exit 1
        fi
        free=`expr $mem_free + $mem_buffer + $mem_cached`
        free=`expr $free / 1024`
    elif [ "$os" == "Darwin" ]; then
        local pages_free=`vm_stat | awk '/Pages free/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " "`
        local pages_inactive=`vm_stat | awk '/Pages inactive/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " "`
        local pages_available=`expr $pages_free + $pages_inactive`
        free=`expr $pages_available \* 4096 / 1024 / 1024`
    else
        echo "Unsupported operating system $os"
        exit 1
    fi
    echo $free
}

function calc_xmx() {
    local min_mem=$1
    local max_mem=$2
    # Get machine available memory
    local free=`free_memory`
    local half_free=$[free/2]

    local xmx=$min_mem
    if [[ "$free" -lt "$min_mem" ]]; then
        exit 1
    elif [[ "$half_free" -ge "$max_mem" ]]; then
        xmx=$max_mem
    elif [[ "$half_free" -lt "$min_mem" ]]; then
        xmx=$min_mem
    else
        xmx=$half_free
    fi
    echo $xmx
}

function remove_with_prompt() {
    local path=$1
    local tips=""

    if [ -d "$path" ]; then
        tips="Remove directory '$path' and all sub files [y/n]?"
    elif [ -f "$path" ]; then
        tips="Remove file '$path' [y/n]?"
    else
        return 0
    fi

    read -p "$tips " yn
    case $yn in
        [Yy]* ) rm -rf "$path";;
        * ) ;;
    esac
}

function ensure_path_writable() {
    local path=$1
    # Ensure input path exist
    if [ ! -d "${path}" ]; then
        mkdir -p ${path}
    fi
    # Check for write permission
    if [ ! -w "${path}" ]; then
        echo "No write permission on directory ${path}"
        exit 1
    fi
}

function get_ip() {
    local os=`uname`
    local loopback="127.0.0.1"
    local ip=""
    case $os in
        Linux)
            if command_available "ifconfig"; then
                ip=`ifconfig | grep 'inet addr:' | grep -v "$loopback" | cut -d: -f2 | awk '{ print $1}'`
            elif command_available "ip"; then
                ip=`ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | awk -F"/" '{print $1}'`
            else
                ip=$loopback
            fi
            ;;
        FreeBSD|OpenBSD|Darwin)
            if command_available "ifconfig"; then
                ip=`ifconfig | grep -E 'inet.[0-9]' | grep -v "$loopback" | awk '{ print $2}'`
            else
                ip=$loopback
            fi
            ;;
        SunOS)
            if command_available "ifconfig"; then
                ip=`ifconfig -a | grep inet | grep -v "$loopback" | awk '{ print $2} '`
            else
                ip=$loopback
            fi
            ;;
        *) ip=$loopback;;
    esac
    echo $ip
}

function download() {
    local path=$1
    local link_url=$2

    if command_available "wget"; then
        wget --help | grep -q '\--show-progress' && progress_opt="-q --show-progress" || progress_opt=""
        wget ${link_url} -P ${path} $progress_opt
    elif command_available "curl"; then
        curl ${link_url} -o ${path}/${link_url}
    else
        echo "Required wget or curl but they are unavailable"
        exit 1
    fi
}

function ensure_package_exist() {
    local path=$1
    local dir=$2
    local tar=$3
    local link=$4

    if [ ! -d ${path}/${dir} ]; then
        if [ ! -f ${path}/${tar} ]; then
            echo "Downloading the compressed package '${tar}'"
            download ${path} ${link}
            if [ $? -ne 0 ]; then
                echo "Failed to download, please ensure the network is available and link is valid"
                exit 1
            fi
            echo "[OK] Finished download"
        fi
        echo "Unzip the compressed package '$tar'"
        tar -zxvf ${path}/${tar} -C ${path} >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo "Failed to unzip, please check the compressed package"
            exit 1
        fi
        echo "[OK] Finished unzip"
    fi
}

###########################################################################

function wait_for_shutdown() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    echo -n "Killing $process_name(pid $pid)" >&2
    while [ $now_s -le $stop_s ]; do
        echo -n .
        process_status "$process_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "OK"
            return 0
        fi
        sleep 2
        now_s=`date '+%s'`
    done
    echo "$process_name shutdown timeout(exceeded $timeout_s seconds)" >&2
    return 1
}

function process_status() {
    local process_name="$1"
    local pid="$2"

    ps -p "$pid"
    if [ $? -eq 0 ]; then
        echo "$process_name is running with pid $pid"
        return 0
    else
        echo "The process $process_name does not exist"
        return 1
    fi
}

function kill_process() {
    local process_name="$1"
    local pid="$2"

    if [ -z "$pid" ]; then
        echo "The process $pid does not exist"
        return 0
    fi

    case "`uname`" in
        CYGWIN*) taskkill /F /PID "$pid" ;;
        *)       kill "$pid" ;;
    esac
}

function kill_process_and_wait() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    kill_process "$process_name" "$pid"
    wait_for_shutdown "$process_name" "$pid" "$timeout_s"
}
