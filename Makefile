up:
	source env/bin/activate

ipfs: 
	docker-compose up -d ipfs 
freeze_env:
	pip freeze > requirements.txt