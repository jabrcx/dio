all:
	python test_lazydict.py
	python test_pipeline.py
	./test_cli.sh > test_cli.sh.out.current
	diff test_cli.sh.out.reference test_cli.sh.out.current
	rm test_cli.sh.out.current
	@echo SUCCESS
