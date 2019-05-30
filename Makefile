PROJECT=openarc

# Build environment
ENV!=echo ${.TARGETS} | awk '{print $$1}'
EXECDIR!=pwd
BUILD=${EXECDIR}/build
BUILD_PKG=${BUILD}/pkg/freebsd
BUILD_FRIEZE=${BUILD}/env/${ENV}

# Configurations
CFGSRC?=${BUILD}/env/${ENV}/cfg
CFGUSR!=dirname ~/.config/test
CFGAPP!=dirname ${CFGUSR}/${PROJECT}/${ENV}/${ENV}
CFGFRIEZE!=dirname ~/.frieze/cfg/test


# Configurations
CFGDIR!=realpath ${BUILD}/local/cfg

# Database
PSQL?=/usr/local/bin/psql
DBNAME=${PROJECT}
PYTHON=/usr/local/bin/python3
PYTEST_FILE_PATTERN?="*_test.py"

############################################## Build targets
cfgset:
	rm -rf ${CFGAPP} ${CFGFRIEZE}
	mkdir -p ${CFGAPP} ${CFGFRIEZE}

	# Set up configurations
.if ${ENV}==test
	# OpenARC
	cp ${CFGSRC}/openarc.conf        ${CFGUSR}

	# Database
	cp ${CFGSRC}/postgresql.conf     ${CFGAPP}
	cp ${CFGSRC}/pg_hba.conf         ${CFGAPP}
	cp ${CFGSRC}/pg_bouncer.ini      ${CFGAPP}
	cp ${CFGSRC}/pg_init.sh          ${CFGAPP}

	# Testing database
	(cd ~\
		&& export db=${PROJECT}_test\
		&& make pgstart\
		&& make cfg=${CFGAPP}/postgresql.conf pgcfgadd\
		&& make cfg=${CFGAPP}/pg_hba.conf     pgcfgadd\
		&& make cfg=${CFGAPP}/pg_bouncer.ini  pgcfgadd\
		&& make cfg=${CFGAPP}/pg_init.sh      pgcfgadd\
		&& export pg_snap=0\
		&& make pginit\
		&& make pgstart)
.elif ${ENV}==integrate
.elif ${ENV}==stage
.elif ${ENV}==prod
.endif

test: cfgset
	${PYTHON} -m unittest discover ./${PROJECT}/tests -p ${PYTEST_FILE_PATTERN}

clean:
	@rm ./${PROJECT}/*.pyc
	@rm ./${PROJECT}/tests/*.pyc
