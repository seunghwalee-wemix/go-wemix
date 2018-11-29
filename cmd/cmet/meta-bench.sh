#!/bin/bash


# environment variables and corresponding flags
# RC_FILE: -r <file>
# META_DIR: -m <dir>, metadium directory
# META_DEV: -d <dev>, device to collect io stats, i.e. nvme1n1
# SWITCH_CONTRACTS: -w, whether to switch contracts after a turn, 0 | 1
# CMET_CONTRACT: -c <address>, contract address when SWITCH_CONTRACTS is off
# CMET_ACCOUNT: -a <password> <file>, account password and file to use
# CMET_ABI: -b <js-file-name>
# CMET_GAS: -g <gas>
# CMET_URL: -s <url>
# LOOP_COUNT PER_LOOP: -l <loop-count> <per-loop>
# PREFIX: -p <prefix>

# BATCH_SIZE: 

[ "$SWITCH_CONTRACTS" = "" ] && SWITCH_CONTRACTS=0
[ "$BATCH_SIZE" = "" ] && BATCH_SIZE=100
[ "$PREFIX" = "" ] && PREFIX=meta
[ "$LOOP_COUNT" = "" ] && LOOP_COUNT=1000
[ "$PER_LOOP" = "" ] && LOOP_COUNT=1000000

function die ()
{
    echo $*
    exit 1;
}

function header ()
{
    echo "@,Date,Elap,Start,End,TPS,DU(KB),Buf,R(KB),R(KB/s),W(KB),W(KB/s),DB/R(#),DB/R(KB),DB/RPS,DB/W(#),DB/W(KB),DB/WPS"
}

function usage ()
{
    echo "Usage: `basename $0` <options> <prefix> <loop-count> <per-loop>...."
}

function main ()
{
    [ "${CMET_ACCOUNT}" ] && export CMET_ACCOUNT;
    [ "${CMET_ABI}" ] && export CMET_ABI;
    [ "${CMET_GAS}" ] && export CMET_GAS;
    [ "${CMET_URL}" ] && export CMET_URL;
    [ "${CMET_CONTRACT}" ] && export CMET_CONTRACT;

    header;

    IX=1;
    for i in $(seq 1 $LOOP_COUNT); do
        EIX=$(($IX + $PER_LOOP - 1))
        echo "### $i: Sending ${PREFIX}-${IX}...${PREFIX}-${EIX}... - $(date)"

        if [ ! "$SWITCH_CONTRACTS" = "0" ]; then
            echo -n "Deploying a new contract..."
            ADDR=$(${META_DIR}/bin/cmet deploy ${CMET_ABI} | awk '/Contract mined/ { print $4 }')
            [ "$ADDR" = "" ] && die "Cannot deploy a new contract"
            export CMET_CONTRACT=$ADDR
            echo "$CMET_CONTRACT";
        fi

        STAT_START=$(${META_DIR}/bin/gmet attach ipc:${META_DIR}/geth.ipc --exec "debug.dbStats(\"${META_DEV}\", true)")
        T_START=$(date +%s)

        FN=/tmp/junk.$$
        ${META_DIR}/bin/cmet -q bulk-kv-put $PREFIX $IX $EIX $BATCH_SIZE | tee $FN
        TPS=$(cat $FN | awk '/^Took/ { print $6 }')
        rm $FN

        T_END=$(date +%s)
        STAT_END=$(${META_DIR}/bin/gmet attach ipc:${META_DIR}/geth.ipc --exec "debug.dbStats(\"${META_DEV}\", 1)")
        DU=$(du -sk ${META_DIR}/geth/chaindata/ 2> /dev/null | awk '{print $1}')
        BUFFER_CACHE=$(free | awk '/^Mem:/ { print $6 }')

        echo "" | awk -v si=$IX -v ei=$EIX -v tps=$TPS -v ts=$T_START -v te=$T_END -v ss="$STAT_START" -v se="$STAT_END" -v du=$DU -v bc=$BUFFER_CACHE '{
  gsub("\\[|\\]|,", "", ss);
  gsub("\\[|\\]|,", "", se);
  if (split(ss, a) != 10 || split(se, b) != 10) {
    print "Not good:"
    print ss
    print se
    print du
  } else {
    if ((dt = te - ts) <= 0)
      dt = 1;

    rc = b[1] - a[1];
    rb = int((b[2] - a[2]) / 1024);
    rps = int(rb / dt);
    wc = b[3] - a[3];
    wb = int((b[4] - a[4]) / 1024);
    wps = int(wb / dt);

    drc = b[5] - a[5];
    drb = int((b[6] - a[6]) / 1024);
    drps = int(drc / dt);
    dwc = b[7] - a[7];
    dwb = int((b[8] - a[8]) / 1024);
    dwps = int(dwc / dt);

    print "@," ts "," dt "," si "," ei "," tps "," du "," bc "," rb "," rps "," wb "," wps "," drc "," drb "," drps "," dwc "," dwb "," dwps;
  }
}';

        IX=$(($IX + $PER_LOOP))
    done
}


while getopts "a:b:c:d:g:hl:m:p:r:s:w" OPTC; do
    case "${OPTC}" in
    a)
        eval NOPTARG="\$${OPTIND}"
        export CMET_ACCOUNT="$OPTARG $NOPTARG"
        OPTIND=$(($OPTIND + 1))
        ;;
    b)
        export CMET_ABI=$OPTARG
        ;;
    c)
        export CMET_CONTRACT=$OPTARG
        ;;
    d)
        META_DEV=$OPTARG
        ;;
    g)
        export CMET_GAS=$OPTARG
        ;;
    h)  
        usage; exit 1;
        ;;
    l)
        eval PER_LOOP="\$${OPTIND}"
        LOOP_COUNT=$OPTARG
        OPTIND=$(($OPTIND + 1))
        ;;
    m)
        META_DIR=$OPTARG
        ;;
    p)
        PREFIX=$OPTARG
        ;;
    r)
        RC_FILE=$OPTARG
        ;;
    s)
        CMET_URL=$OPTARG
        ;;
    w)
        SWITCH_CONTRACTS=1
        ;;
    *)
        usage; exit 1;
        ;;
    esac
done

[ -f "${RC_FILE}" ] && source "${RC_FILE}"

shift $(($OPTIND - 1))
if [ $# = 3 ]; then
    PREFIX=$1
    LOOP_COUNT=$2
    PER_LOOP=$3
fi

if [ ! -x "${META_DIR}/bin/cmet" ]; then
    echo "Cannot find metadium dir with cmet in."
    usage
    exit 1
fi
if [ ! -f "${CMET_ABI}" ]; then
    echo "Cannot find contract javascript file."
    usage
    exit 1
fi
if [ "$PREFIX" = "" -o "$LOOP_COUNT" = "" -o "$PER_LOOP" = "" ]; then
    echo "Invalid Arguments"
    usage
    exit 1
fi

# it begins
main;

# EOF
