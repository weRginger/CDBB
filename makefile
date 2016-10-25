EXECS=cdbb nocdbb
MPICC=mpicc

all: ${EXECS}

cdbb: cdbb.c
	${MPICC} -o cdbb cdbb.c
	
nocdbb: nocdbb.c
	${MPICC} -o nocdbb nocdbb.c

clean:
	rm -f ${EXECS}
