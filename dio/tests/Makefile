all:
	python test_lazydict.py
	python test_pipeline.py
	test `./test_cli.sh | md5sum | awk '{print $$1}'` = 04ef0cbe1f4d930610d15d59278f737c
	@echo SUCCESS
