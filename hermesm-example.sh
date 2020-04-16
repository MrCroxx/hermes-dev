#!/bin/bash

# customize cluster
cluster=('user@host' 'user@host' 'user@host' 'user@host')

case $1 in
        'start')
                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "start hermes on ${host}"
                        ssh $host screen -S hermes -dm "~/hermes -c ~/hermes-${i}.yaml"
                done
                ;;
        'stop')
                for host in ${cluster[*]}
                do
                        echo "stop hermes on ${host}"
                        ssh $host screen -S hermes -X quit
                done
                ;;
        'ls')
                for host in ${cluster[*]}
                do
                        if [[ $(sed '1d' <<< $(ssh $host 'screen -ls')) =~ 'hermes' ]]
                        then
                                echo "hermes running on ${host}"
                        fi
                done
                ;;
        'deploy')
                cd $GOPATH/src/mrcroxx.io/hermes
                go build mrcroxx.io/hermes
                go install mrcroxx.io/hermes

                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "deploy hermes on ${host}"
                        scp -q $GOPATH/bin/hermes ${host}:~/
                        scp -q ~/hermes-${i}.yaml ${host}:~/
                        scp -q -r $GOPATH/src/mrcroxx.io/hermes/ui ${host}:~/
                done
                ;;
        'clear')
                i=0
                for host in ${cluster[*]}
                do
                        let i++
                        echo "clear hermes on ${host}"
                        ssh $host rm -rf ~/hermes-${i} ~/hermes ~/hermes-${i}.yaml ~/ui
                done
                ;;
        *)
                echo "start | stop | ls | deploy | clear"
                ;;
esac