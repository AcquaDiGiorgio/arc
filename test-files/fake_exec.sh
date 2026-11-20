# Should contain the following env vars:
# - JOB_FIFO: Named pipe waiting for the execution
. ./env

output_dir=$(cat $JOB_FIFO)
rm env
rm -- "$0"
mv $output_dir/* .
