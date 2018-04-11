DBRUNDIR?=~/run/db
DBCFGDIR?=./cfg/
DBSOCKDIR?=/tmp
DBLOGDIR?=/tmp
PGCTL?=/usr/local/bin/pg_ctl
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
	@${PGCTL} init -D ${DBRUNDIR}/${PROJECT}
	@cp ${DBCFGDIR}/*.conf ${DBRUNDIR}/${PROJECT}
	@${PGCTL} -D ${DBRUNDIR}/${PROJECT} -l ${DBLOGDIR}/logfile start
	@sleep 3

dbcreate:
	-dropdb -h ${DBSOCKDIR} ${PROJECT}
	createdb -h ${DBSOCKDIR} ${PROJECT}

dbinit: dbcreate

dbhardinit: dbmsinit dbinit

test:
	# Todo: replace this with TAP output
	@echo "Running tests"
	@export OPENARC_CFG_DIR=./cfg && python3 -m unittest discover ./openarc/tests -p ${PYTEST_FILE_PATTERN}

testclean: dbrefresh test
