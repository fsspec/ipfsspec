ipfd:
	source	ls ./env/bin/activate
ipfs: 
	docker-compose up -d ipfs 
freeze_env:
	pip freeze > requirements.txt
test:
	python test/test_ipfs_async.py