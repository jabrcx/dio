non-gui: basics math
	@echo SUCCESS

basics:
	python test_lazydict.py
	python test_pipeline.py
	./test_cli.sh > test_cli.sh.out.current
	diff test_cli.sh.out.reference test_cli.sh.out.current
	rm test_cli.sh.out.current

math:
	./test_cli_math.sh > test_cli_math.sh.out.current
	diff test_cli_math.sh.out.reference test_cli_math.sh.out.current
	rm test_cli_math.sh.out.current

plot:
	./test_cli_plotmpl.sh
