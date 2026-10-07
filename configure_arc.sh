#!/bin/sh
set +x -e

USEJWT=0

useradd grid

echo Creating certs
arcctl test-ca init
arcctl test-ca usercert -i root --validity 360 --force
cat /root/.globus/*.pem > /root/.globus/cert.pem
sed -i 's/nobody/grid/g' /etc/arc.conf.d/10-testCA-access.conf

if (( "$USEJWT" == 1 )); then
    echo Initializing servers
    /usr/share/arc/arc-arex-start
    /usr/share/arc/arc-arex-ws-start

    echo Initializing jwt token as grid
    su -c "arcctl test-jwt init -f | tail -n 1 > /tmp/arc_deploy" - grid

    echo Executing arc_deploy
    chmod u+x /tmp/arc_deploy
    sh /tmp/arc_deploy

    echo Killing arched
    pkill arched

    echo Waiting until they are dead
    while pgrep arched >/dev/null 2>/dev/null; do
        printf "\r%-${COLUMNS}s" "Waiting"
        sleep 1
        printf "\r%-${COLUMNS}s" "Waiting."
        sleep 1
        printf "\r%-${COLUMNS}s" "Waiting.."
        sleep 1
        printf "\r%-${COLUMNS}s" "Waiting..."
        sleep 1
    done
    printf "\r%-${COLUMNS}s" "Done"
fi

mkdir -p /var/spool/arc/sessiondir
mkdir -p /var/spool/arc/jobstatus

arcproxy -c validityPeriod=9999999

identity=$(arcproxy -I | grep Identity | cut -d " " -f 2-)
sed -i "s|sender_dn =|sender_dn = ${identity}|" /etc/arc.conf
sed -i "s|dry_run = yes|dry_run = false|" /etc/arc.conf

echo Launching servers
/usr/share/arc/arc-arex-start
/usr/share/arc/arc-arex-ws-start
