#!/usr/bin/env bash
# One-shot AWS run, driven from a laptop (docs/AWS-RUN.md). Needs `aws login` done first.
#
#   run.sh preflight   identity, quotas, plan limits (run-instances --dry-run), AMI, default VPC
#   run.sh all         preflight, launch, setup, smoke gate, 3-region probe, main run, pull, teardown
#   run.sh tune        preflight, one main host, setup, smoke, host.sh tune (A/B rounds), pull, teardown
#   run.sh pull        copy ~/results from every live host into $OUT
#   run.sh teardown    terminate everything tagged Project=voluntas-run, delete its SGs and keys
#
# Everything is tagged, every instance halts itself (and so terminates) after LIFE minutes even if
# this laptop goes away, and teardown runs on any exit of `all` unless KEEP=1.
set -u
cd "$(dirname "$0")/../.."
export AWS_PROFILE=${AWS_PROFILE:-default} AWS_PAGER=""
MAIN_REGION=${MAIN_REGION:-eu-west-2}; MAIN_TYPE=${MAIN_TYPE:-c7i.xlarge}
PROBES=${PROBES:-"eu-west-1 us-east-1"}; PROBE_TYPE=${PROBE_TYPE:-c7i-flex.large}
LIFE_MAIN=${LIFE_MAIN:-240}; LIFE_PROBE=${LIFE_PROBE:-75}
TAG=voluntas-run
STATE=${STATE:-$HOME/.cache/voluntas-run} # holds the ssh key: never inside the repo
OUT=${OUT:-$PWD/bench/aws-$(date +%F)}
mkdir -p "$STATE" "$OUT"
KEY=$STATE/key
SSH="ssh -i $KEY -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$STATE/known_hosts -o ConnectTimeout=10 -o ServerAliveInterval=30 -o BatchMode=yes"
SCP="scp -q -i $KEY -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$STATE/known_hosts -o BatchMode=yes"
log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT/driver.log"; }
die() { log "FATAL: $*"; exit 1; }
ip_of() { awk -v r="$1" '$1==r {print $3}' "$STATE/hosts"; }
on() { local r=$1; shift; $SSH ubuntu@"$(ip_of $r)" "$@"; }
ami() { aws ssm get-parameter --region $1 --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text; }

preflight() {
  log "== preflight"
  aws sts get-caller-identity --query Arn --output text > /dev/null || die "no AWS credentials: run 'aws login --profile $AWS_PROFILE'"
  log "identity $(aws sts get-caller-identity --query Account --output text)"
  local ok=1 r t need q msg plan
  # run-instances --dry-run does not apply the Free plan's instance-type limit; ask directly.
  plan=$(aws freetier get-account-plan-state --region us-east-1 --query accountPlanType --output text 2>/dev/null) || plan=?
  log "account plan $plan, credits $(aws freetier get-account-plan-state --region us-east-1 --query accountPlanRemainingCredits.amount --output text 2>/dev/null)"
  for r in $MAIN_REGION $PROBES; do
    if [ $r = $MAIN_REGION ]; then t=$MAIN_TYPE; else t=$PROBE_TYPE; fi
    need=$(aws ec2 describe-instance-types --region $r --instance-types $t --query 'InstanceTypes[0].VCpuInfo.DefaultVCpus' --output text 2>/dev/null) || need=?
    q=$(aws service-quotas get-service-quota --region $r --service-code ec2 --quota-code L-1216C47A --query Quota.Value --output text 2>/dev/null) || q=?
    msg=$(aws ec2 run-instances --dry-run --region $r --image-id "$(ami $r)" --instance-type $t --count 1 2>&1)
    case "$msg" in
      *DryRunOperation*) msg=launchable ;;
      *) ok=0; msg="NOT launchable: $(echo "$msg" | tail -1 | cut -c1-200)" ;;
    esac
    local vpc; vpc=$(aws ec2 describe-vpcs --region $r --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text 2>/dev/null)
    [ "$vpc" = None ] || [ -z "$vpc" ] && { ok=0; vpc="NONE (create one: aws ec2 create-default-vpc --region $r)"; }
    if [ "$plan" = FREE ] && [ "$(aws ec2 describe-instance-types --region $r --instance-types $t --query 'InstanceTypes[0].FreeTierEligible' --output text 2>/dev/null)" != True ]; then
      ok=0; msg="NOT launchable: $t is not free-tier eligible and the account is on the Free plan"
    fi
    log "$r $t vcpus=$need quota=$q vpc=$vpc $msg"
    [ "$q" != ? ] && [ "$need" != ? ] && [ "${q%.*}" -lt "$need" ] && { ok=0; log "$r: quota $q < $need vCPU"; }
  done
  [ $ok = 1 ] || die "preflight failed (a Free-plan account can only launch free-tier types; upgrading to the Paid plan keeps the credits)"
  log "preflight OK"
}

launch() { # $1 region $2 type $3 life-min $4 disk-GB $5 role
  local r=$1 t=$2 life=$3 disk=$4 role=$5 sg id ip
  aws ec2 describe-key-pairs --region $r --key-names $TAG >/dev/null 2>&1 \
    || aws ec2 import-key-pair --region $r --key-name $TAG --public-key-material fileb://$KEY.pub --tag-specifications "ResourceType=key-pair,Tags=[{Key=Project,Value=$TAG}]" >/dev/null \
    || die "$r: import-key-pair"
  sg=$(aws ec2 describe-security-groups --region $r --filters Name=group-name,Values=$TAG --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
  if [ "$sg" = None ] || [ -z "$sg" ]; then
    sg=$(aws ec2 create-security-group --region $r --group-name $TAG --description "$TAG ssh" --tag-specifications "ResourceType=security-group,Tags=[{Key=Project,Value=$TAG}]" --query GroupId --output text) || die "$r: create-security-group"
  fi
  aws ec2 authorize-security-group-ingress --region $r --group-id $sg --protocol tcp --port 22 --cidr "$MYIP/32" >/dev/null 2>&1 || true
  printf '#!/bin/bash\nshutdown -h +%s "voluntas-run lifetime"\n' $life > "$STATE/ud-$r"
  id=$(aws ec2 run-instances --region $r --image-id "$(ami $r)" --instance-type $t --key-name $TAG --security-group-ids $sg \
    --instance-initiated-shutdown-behavior terminate --user-data file://$STATE/ud-$r \
    --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=$disk,VolumeType=gp3,Iops=3000,Throughput=250,DeleteOnTermination=true}" \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$TAG},{Key=Name,Value=$TAG-$role}]" "ResourceType=volume,Tags=[{Key=Project,Value=$TAG}]" \
    --query 'Instances[0].InstanceId' --output text) || die "$r: run-instances"
  log "$r launched $id ($t, halts itself in $life min)"
  aws ec2 wait instance-running --region $r --instance-ids $id || die "$r: never running"
  ip=$(aws ec2 describe-instances --region $r --instance-ids $id --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  echo "$r $id $ip $role" >> "$STATE/hosts"
  local i; for i in $(seq 40); do on $r true 2>/dev/null && break; sleep 6; done
  on $r true || die "$r: ssh never came up ($ip)"
  log "$r ssh up at $ip"
}

wait_marker() { # $1 region $2 regex $3 timeout-s: prints the matching marker, fails on timeout
  local r=$1 re=$2 t0=$(date +%s) m
  while [ $(( $(date +%s) - t0 )) -lt $3 ]; do
    m=$(on $r "grep -E '$re' results/markers 2>/dev/null | tail -1" 2>/dev/null) && [ -n "$m" ] && { echo "$m"; return 0; }
    sleep 20
  done
  return 1
}

pull() {
  [ -s "$STATE/hosts" ] || return 0
  while read -r r id ip role; do
    mkdir -p "$OUT/$role-$r"
    rsync -az -e "$SSH" ubuntu@$ip:results/ "$OUT/$role-$r/" 2>/dev/null && log "pulled $role-$r" || log "pull $role-$r failed"
  done < "$STATE/hosts"
}

teardown() {
  log "== teardown"
  local r ids sg
  for r in $MAIN_REGION $PROBES; do
    ids=$(aws ec2 describe-instances --region $r --filters Name=tag:Project,Values=$TAG Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$ids" ]; then
      aws ec2 terminate-instances --region $r --instance-ids $ids >/dev/null && log "$r terminating $ids"
      aws ec2 wait instance-terminated --region $r --instance-ids $ids
    fi
    sg=$(aws ec2 describe-security-groups --region $r --filters Name=group-name,Values=$TAG --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    [ -n "$sg" ] && [ "$sg" != None ] && { aws ec2 delete-security-group --region $r --group-id $sg && log "$r deleted $sg"; }
    aws ec2 delete-key-pair --region $r --key-name $TAG >/dev/null 2>&1
    left=$(aws ec2 describe-instances --region $r --filters Name=tag:Project,Values=$TAG Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text 2>/dev/null)
    log "$r left running: ${left:-?}"
  done
  rm -f "$STATE/hosts"
}

stop_probe() { # terminate one probe host as soon as its data is pulled
  local r=$1 id; id=$(awk -v r=$r '$1==r {print $2}' "$STATE/hosts")
  [ -n "$id" ] && aws ec2 terminate-instances --region $r --instance-ids $id >/dev/null && log "$r probe terminated $id"
  grep -v "^$r " "$STATE/hosts" > "$STATE/hosts.new"; mv "$STATE/hosts.new" "$STATE/hosts"
}

all() {
  preflight
  [ -s "$STATE/hosts" ] && die "$STATE/hosts exists: a previous run is live; run teardown first"
  [ "${KEEP:-0}" = 1 ] || trap 'pull; teardown' EXIT
  trap 'log "interrupted"; exit 130' INT TERM
  MYIP=$(curl -s -m 10 https://checkip.amazonaws.com) || die "no public IP"
  [ -f $KEY ] || ssh-keygen -q -t ed25519 -N "" -C $TAG -f $KEY
  git diff --quiet HEAD -- src Cargo.toml Cargo.lock || log "WARNING: uncommitted changes in src are not shipped"
  log "== launch from $MYIP: main $MAIN_TYPE in $MAIN_REGION, probes $PROBE_TYPE in $PROBES; out $OUT"
  launch $MAIN_REGION $MAIN_TYPE $LIFE_MAIN 100 main &
  local p; for p in $PROBES; do launch $p $PROBE_TYPE $LIFE_PROBE 20 probe & done
  wait
  [ -n "$(ip_of $MAIN_REGION)" ] || die "main host did not come up"
  log "rev $(git rev-parse --short HEAD)"
  git archive HEAD | on $MAIN_REGION 'mkdir -p src && tar -x -C src' || die "ship source"
  $SCP bench/aws/host.sh ubuntu@$(ip_of $MAIN_REGION):host.sh
  on $MAIN_REGION 'chmod +x host.sh; nohup ./host.sh setup main >/dev/null 2>&1 </dev/null &'
  wait_marker $MAIN_REGION 'SETUP_' 1500 | grep -q SETUP_OK || { pull; die "main setup failed (see setup.log)"; }
  log "main built"
  mkdir -p "$STATE/bin"
  $SCP ubuntu@$(ip_of $MAIN_REGION):src/target/release/{collector,verify} "$STATE/bin/" || die "fetch binaries"
  local live=""
  for p in $PROBES; do
    [ -n "$(ip_of $p)" ] || { log "$p: no host, probe skipped"; continue; }
    on $p 'mkdir -p bin' && $SCP "$STATE"/bin/{collector,verify} ubuntu@$(ip_of $p):bin/ && $SCP bench/aws/host.sh ubuntu@$(ip_of $p):host.sh \
      && on $p 'chmod +x host.sh; nohup ./host.sh setup probe >/dev/null 2>&1 </dev/null &' && live="$live $p"
  done
  local probes_ok=$MAIN_REGION
  for p in $live; do wait_marker $p 'SETUP_' 600 | grep -q SETUP_OK && probes_ok="$probes_ok $p" || log "$p: setup failed, probe skipped"; done

  log "== smoke on $probes_ok"
  for r in $probes_ok; do on $r 'nohup ./host.sh smoke >/dev/null 2>&1 </dev/null &'; done
  local go=""
  for r in $probes_ok; do
    if wait_marker $r 'SMOKE_' 300 | grep -q SMOKE_OK; then go="$go $r"; else log "$r: SMOKE FAILED: $(on $r 'tail -5 results/smoke.log' 2>&1 | tr '\n' '|')"; fi
  done
  case " $go " in *" $MAIN_REGION "*) ;; *) pull; die "NO-GO: smoke failed on the main host" ;; esac
  log "GO:$go"

  local start=$(( $(date +%s) + 60 ))
  log "== probe at $(date -r $start +%H:%M:%S) on$go"
  for r in $go; do on $r "nohup ./host.sh probe $start >/dev/null 2>&1 </dev/null &"; done
  sleep $(( ${PROBE_SECS:-600} + 60 ))
  for r in $go; do wait_marker $r 'PROBE_' 600 | tee -a "$OUT/driver.log" >/dev/null || log "$r: probe timed out"; done
  pull
  for p in $PROBES; do stop_probe $p; done

  log "== main run (tests, bench, 60 min full universe, sample)"
  on $MAIN_REGION 'nohup ./host.sh main >/dev/null 2>&1 </dev/null &'
  local t0=$(date +%s) n=0
  until on $MAIN_REGION 'grep -q ALL_DONE results/markers' 2>/dev/null; do
    sleep 60; n=$((n+1))
    [ $(( $(date +%s) - t0 )) -gt ${MAIN_TIMEOUT:-6000} ] && { log "main run timed out"; break; }
    if [ $((n % 10)) = 0 ]; then
      pull
      log "t=$(( ($(date +%s) - t0) / 60 ))m $(on $MAIN_REGION 'tail -1 results/res-main.txt 2>/dev/null; tail -1 results/verify-main.txt 2>/dev/null | cut -c1-160' 2>/dev/null | tr '\n' '|')"
    fi
  done
  pull
  log "markers: $(cat "$OUT"/main-$MAIN_REGION/markers 2>/dev/null | awk '{print $1}' | tr '\n' ' ')"
  log "RUN_DONE"
}

tune() {
  PROBES=""
  preflight
  [ -s "$STATE/hosts" ] && die "$STATE/hosts exists: a previous run is live; run teardown first"
  [ "${KEEP:-0}" = 1 ] || trap 'pull; teardown' EXIT
  trap 'log "interrupted"; exit 130' INT TERM
  MYIP=$(curl -s -m 10 https://checkip.amazonaws.com) || die "no public IP"
  [ -f $KEY ] || ssh-keygen -q -t ed25519 -N "" -C $TAG -f $KEY
  git diff --quiet HEAD -- src Cargo.toml Cargo.lock bench/aws || log "WARNING: uncommitted changes are not shipped"
  log "== tune: $MAIN_TYPE in $MAIN_REGION, rev $(git rev-parse --short HEAD); out $OUT"
  launch $MAIN_REGION $MAIN_TYPE $LIFE_MAIN 100 main
  git archive HEAD | on $MAIN_REGION 'mkdir -p src && tar -x -C src' || die "ship source"
  $SCP bench/aws/host.sh ubuntu@$(ip_of $MAIN_REGION):host.sh
  on $MAIN_REGION 'chmod +x host.sh; nohup ./host.sh setup main >/dev/null 2>&1 </dev/null &'
  wait_marker $MAIN_REGION 'SETUP_' 1500 | grep -q SETUP_OK || { pull; die "setup failed (see setup.log)"; }
  on $MAIN_REGION 'nohup ./host.sh smoke >/dev/null 2>&1 </dev/null &'
  wait_marker $MAIN_REGION 'SMOKE_' 300 | grep -q SMOKE_OK || { pull; die "NO-GO: smoke failed"; }
  log "== tune rounds"
  on $MAIN_REGION 'nohup ./host.sh tune >/dev/null 2>&1 </dev/null &'
  local t0=$(date +%s) n=0
  until on $MAIN_REGION 'grep -qE "TUNE_(DONE|FAILED)" results/markers' 2>/dev/null; do
    sleep 60; n=$((n+1))
    [ $(( $(date +%s) - t0 )) -gt ${MAIN_TIMEOUT:-7200} ] && { log "tune timed out"; break; }
    [ $((n % 10)) = 0 ] && log "t=$(( ($(date +%s) - t0) / 60 ))m $(on $MAIN_REGION 'tail -1 results/markers; tail -1 results/tune.log' 2>/dev/null | tr '\n' '|')"
  done
  pull
  log "markers: $(awk '{print $1}' "$OUT"/main-$MAIN_REGION/markers 2>/dev/null | tr '\n' ' ')"
  log "TUNE_RUN_DONE"
}

case "${1:-}" in
  preflight) preflight ;;
  all) all ;;
  tune) tune ;;
  pull) pull ;;
  teardown) teardown ;;
  *) echo "usage: run.sh preflight | all | tune | pull | teardown"; exit 2 ;;
esac
