#!/bin/bash
get_id() {
    echo $1 | cut -d '_' -f 2
}

get_job_file() {
   echo $1 | cut -d '_' -f 1
}

# Execute each individual job
run_task() {
    task_id=$1
    exe=$2
    group_id=$3
    base_dir=$4

    cd "$task_id"
    
    # Init per job environment
    . ./env
    echo "[${task_id}] Executing task"

    # 'set -e' inside the job execution to obtain the real exit status in case of failure
    bash -e ${exe} $ARGS 1> ${group_id}.out 2> ${group_id}.err

    task_status=$?

    # Report job ending and status
    echo "[${task_id}] Task Finished"
    echo "[${task_id}] Process final status: ${task_status}"

    # if [ $task_status = 0 ]; then
    #     echo "$task_id\n" >> "$base_dir/std.out"
    # else
    #     echo "$task_id\n" >> "$base_dir/std.err"
    # fi
}

# Init group environment (configuration)
#  It must contain 'EXECS' and 'GROUP_ID' vars
. ./groupConfig
export -f run_task
BASEDIR=$(pwd)

touch std.out
touch std.err

for exe in ${EXECS[@]}; do
    [ -f "$exe" ] || exit 1

    jobId=$(get_id ${exe})
    mkdir "${jobId}"
    files="*_${jobId}"

    for filename in $files; do
        [ -f "${filename}" ] || continue
        # Move the job specific files to its directory, removing the jobId from its name
        mv $filename "${jobId}/$(get_job_file $filename)"
    done

    printf "%s\0%s\0%s\0%s\0" "$jobId" "$(get_job_file $exe)" "$GROUP_ID" "$BASEDIR"

done | xargs -0 -n 4 -P 0 bash -c 'run_task "$@"' --
