PROJECT=openarc

# Build environment
EXECDIR!=pwd
BUILD=${EXECDIR}/build
BUILD_PKG=${BUILD}/remote/pkg/freebsd
BUILD_FRIEZE=${BUILD}/remote/frieze

# Configurations
CFGDIR!=realpath ${BUILD}/local/cfg

# Database
PSQL?=/usr/local/bin/psql
DBNAME=${PROJECT}
PYTHON=/usr/local/bin/python3
PYTEST_FILE_PATTERN?="*_test.py"

clean:
	@rm ./${PROJECT}/*.pyc
	@rm ./${PROJECT}/tests/*.pyc

dbcfg:
	(cd ~ && make cfg=${CFGDIR}/postgresql.conf pgcfgadd)
	(cd ~ && make cfg=${CFGDIR}/pg_hba.conf     pgcfgadd)
	(cd ~ && make cfg=${CFGDIR}/pg_bouncer.ini  pgcfgadd)
	cp ${CFGDIR}/openarc.conf   ~/.config/

dbstop:
	(cd ~ && make db=${DBNAME} pgstop)

dbstart: dbcfg
	(cd ~ && make db=${DBNAME} pgstart)
	${PSQL} -d ${DBNAME} < ${CFGDIR}/pg_init.sql

dbinit: dbcfg
	createdb -h /tmp ${DBNAME}

test: dbstart
	# Todo: replace this with TAP output
	@echo "Running tests"
	${PYTHON} -m unittest discover ./${PROJECT}/tests -p ${PYTEST_FILE_PATTERN}
