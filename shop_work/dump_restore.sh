#!/usr/bin/env bash

remote_dir=/var/ftp/pub/mongo_dump
remote_addr=ip
remote_port=22
remote_user=dbbak
backup_name=${2}_${4}.`date +"%Y%m%d%H%M%S"`

do_dump() {
    f_name=${backup_name}.tgz
    ssh -l ${remote_user} ${remote_addr} -- "/usr/bin/env mongodump --host $1 --port $2 --db=$3 --archive=${backup_name} && /usr/bin/tar zcf ${remote_dir}/$f_name ${backup_name} && /usr/bin/rm -f ${backup_name}"
}

do_restore() {
    ssh -l ${remote_user} ${remote_addr} -- "cd ${remote_dir} && tar xf $4 -O | /usr/bin/env mongorestore --host $1 --port $2 --db=$3 --archive --drop"
}

case $1 in
dump)
    do_dump $2 $3 $4
    ;;
restore)
    do_restore $2 $3 $4 $5
    ;;
*)
    echo "Usage: $0 dump|restore"
esac
