
run_task() {
    task_id=$1
    input=${2#${task_id}_*}
    bundle_id=$3
    base_dir=$4

    cd "$task_id"

    echo "[${task_id}] Executing task"

    # 'set -e' inside the job execution to obtain the real exit status in case of failure
    bash -e ${input} \\
            1> ${bundle_id}.out \\
            2> ${bundle_id}.err

    task_status=$?

    # Report job ending and status
    echo "[${task_id}] Task Finished"
    echo "[${task_id}] Process final status: ${task_status}"
}

export -f run_task

for input in ${INPUT[@]}; do
    [ -f "$input" ] || break

    jobId=$(get_id ${input})
    mkdir ${jobId}

    for filename in ${jobId}*; do
        [ -f ${filename} ] || continue
        # Move the job specific files to its directory, removing the jobId from its name
        mv $filename ${jobId}/${filename#${jobId}_*}
    done

    printf "%s\0%s\0%s\0%s\0" "$jobId" "$input" "$BUNDLE_ID" "$BASEDIR"

done | xargs -0 -n 4 -P 0 bash -c 'run_task "$@"' -- 
