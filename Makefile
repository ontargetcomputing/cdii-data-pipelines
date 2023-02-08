execute-helloworld:
	dbx execute "hello-world-job" --cluster-name="Richard's Cluster" --environment ci

execute-bronze:
	dbx execute "bronze-task" --cluster-name="Richard's Cluster" --environment ci	 --task bronze

test-unit:
	pytest tests/unit --cov