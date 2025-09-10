#!/bin/bash

check_invalid_chars() {
  local attr_name=$1
  local invalid_chars=$2
  if [[ "$invalid_chars" != "" ]]; then
    echo "[ERROR] Invalid value \"$string_attr\" for attribute $attr_name!" 1>&2
    exit 1
  fi
}

validate_integer_attr() {
  local attr_name=$1
  local string_attr=$2
  local invalid_chars=$(echo $string_attr | sed -e "s/[[:digit:]]//g")
  check_invalid_chars $attr_name $invalid_chars
}

validate_string_attr() {
  local attr_name=$1
  local string_attr=$2
  local invalid_chars=$(echo $string_attr | sed -e "s/[[:alnum:]]//g" \
                                                -e "s/\(.\|-\|_\)//g")
  check_invalid_chars $attr_name $invalid_chars
}

validate_list_attr() {
  local attr_name=$1
  local string_attr=$2
  local invalid_chars=$(echo $string_attr | sed -e "s/\,//g")
  validate_string_attr $attr_name $invalid_chars
}

run_bucardo_command() {
  local comm=$1
  su - postgres -c "bucardo $comm"
}

start_postgres() {
  service postgresql start
  local status=false
  while [[ $status == false ]]; do
    [[ $(run_bucardo_command "status") ]] && status=true
    sleep 5
  done
}

db_attr() {
  local database=$1
  local attr=$2
  local attr_type=$3
  local value="$(jq ".databases[$database].$attr" /media/bucardo/bucardo.json)"
  case "$attr_type" in
    "string") validate_string_attr "$attr" "$value" ;;
    "list") validate_list_attr "$attr" "$value" ;;
    "integer") validate_integer_attr "$attr" "$value" ;;
    *) echo "Invalid type for $attr." 1>&2; exit 2;;
  esac
  echo $value
}

sync_attr() {
  local sync=$1
  local attr=$2
  local attr_type=$3
  local value="$(jq ".syncs[$sync].$attr" /media/bucardo/bucardo.json)"
  case "$attr_type" in
    "string") validate_string_attr "$attr" "$value" ;;
    "list") validate_list_attr "$attr" "$value" ;;
    "integer") validate_integer_attr "$attr" "$value" ;;
    *) echo "Invalid type for $attr." 1>&2; exit 2;;
  esac
  echo $value
}

one_time_copy_attr() {
  local sync_index=$1
  local value=$(sync_attr $sync_index onetimecopy integer)
  local invalid_chars=$(echo $value | sed -e "s/[0,1,2]//g")
  check_invalid_chars onetimecopy $invalid_chars
  echo $value
}

load_db_pass() {
  local database=$1
  local pass=$(db_attr $database pass string)
  local id=$(db_attr $database id integer)
  if [[ "$pass" == "\"env\"" ]]; then
    echo "$(env | grep "BUCARDO_DB$id" | cut -d'=' -f2)"
  else
    echo "$pass"
  fi
}

add_databases_to_bucardo() {
  echo "[CONTAINER] Adding databases to Bucardo..."
  local db_id db_pass db_port
  local db_index=0
  NUM_DBS=$(jq '.databases | length' /media/bucardo/bucardo.json)
  while [[ $db_index -lt $NUM_DBS ]]; do
    echo "[CONTAINER] Adding db $db_index"
    db_id=$(db_attr $db_index id integer)
    db_pass=$(load_db_pass $db_index)

    run_bucardo_command "del db db$db_id --force"

    local add_db_cmd="add db db$db_id --force"
    add_db_cmd="$add_db_cmd dbname=\"$(db_attr $db_index dbname string)\""
    add_db_cmd="$add_db_cmd user=\"$(db_attr $db_index user string)\""
    add_db_cmd="$add_db_cmd pass=\"$db_pass\""
    add_db_cmd="$add_db_cmd host=\"$(db_attr $db_index host string)\""

    db_port=$(db_attr $db_index port integer)

    if [[ "$db_port" != "null" ]]; then
      add_db_cmd="$add_db_cmd port=\"$db_port\""
    fi

    run_bucardo_command "$add_db_cmd" || exit 2
    db_index=$(expr $db_index + 1)
  done
}

db_sync_entities() {
  local sync_index=$1
  local entity=$2
  local db_index=0
  local sync_entity

  sync_entity=$(sync_attr $sync_index $entity"s[$db_index]" string)
  while [[ "$sync_entity" != null ]]; do
    [[ "$DB_STRING" != "" ]] && DB_STRING="$DB_STRING,"
    DB_STRING=$DB_STRING"db"$sync_entity":$entity"
    db_index=$(expr $db_index + 1)
    sync_entity=$(sync_attr $sync_index $entity"s[$db_index]" string)
  done
}

db_sync_string() {
  local sync_index=$1
  DB_STRING=""
  db_sync_entities $sync_index "source"
  db_sync_entities $sync_index "target"
}

add_syncs_to_bucardo() {
  local sync_index=0
  local num_syncs=$(jq '.syncs | length' /media/bucardo/bucardo.json)
  while [[ $sync_index -lt $num_syncs ]]; do
    echo "[CONTAINER] Adding sync$sync_index to Bucardo..."
    db_sync_string $sync_index
    local one_time_copy="$(one_time_copy_attr $sync_index)"
    
    run_bucardo_command "del sync sync$sync_index"

    local herd_name=$(sync_attr $sync_index herd string)

    if [[ "$herd_name" != "null" ]]; then
      echo "[CONTAINER] Using herd: $herd_name"
      local source_db_id=$(sync_attr $sync_index "sources[0]" integer)
      
      run_bucardo_command "del herd $herd_name"
      run_bucardo_command "add herd $herd_name"
      run_bucardo_command "add all tables --herd=$herd_name db=db$source_db_id"

      run_bucardo_command "add sync sync$sync_index \
                           herd=$herd_name \
                           dbs=$DB_STRING \
                           onetimecopy=$one_time_copy" || exit 2
    else
      echo "[CONTAINER] Using table list"
      run_bucardo_command "add sync sync$sync_index \
                           dbs=$DB_STRING \
                           tables=$(sync_attr $sync_index tables list) \
                           onetimecopy=$one_time_copy" || exit 2
    fi
    sync_index=$(expr $sync_index + 1)
  done
}


start_bucardo() {
  echo "[CONTAINER] Starting Bucardo..."
  run_bucardo_command "start --verbose"
}

bucardo_status() {
  echo "[CONTAINER] Now, some status for you."
  local run=true
  while [[ $run ]]; do
    run_bucardo_command "status"
    sleep 15
  done
}

main() {
  start_postgres 2> /dev/null
  add_databases_to_bucardo
  add_syncs_to_bucardo
  start_bucardo
  bucardo_status
}

main