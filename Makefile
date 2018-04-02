DBRUNDIR?=~/run/db
DBCFGDIR?=./cfg/
PYTEST_BIN?="python -m unittest discover"
PYTEST_FILE_PATTERN?="*_test.py"
PROJECT=openarc

clean:
	@rm ./${PROJECT}/*.pyc
	@rm ./${PROJECT}/tests/*.pyc

dbmsinit:
	-@pkill postgres
	@rm -rf /tmp/.s.PGSQL*
	@rm -rf ${DBRUNDIR}/${PROJECT}
	@/bin/mkdir -p ${DBRUNDIR}/${PROJECT}
	@pg_ctl init -D ${DBRUNDIR}/${PROJECT}
	@cp ${DBCFGDIR}/*.conf ${DBRUNDIR}/${PROJECT}
	@pg_ctl -D ${DBRUNDIR}/${PROJECT} -l /tmp/logfile start
	@sleep 10

dbcreate:
	-dropdb ${PROJECT}
	createdb ${PROJECT}

dbinit: dbcreate

dbhardinit: dbmsinit dbinit

test:
	# Todo: replace this with TAP output
	@echo "Running tests"
	@export OPENARC_CFG_DIR=./cfg && python3 -m unittest discover ./openarc/tests -p ${PYTEST_FILE_PATTERN}

testclean: dbrefresh test
