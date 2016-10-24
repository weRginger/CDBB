EXECS=cdbb
MPICC?=mpicc

all: ${EXECS}

cdbb: cdbb.c
	${MPICC} -o cdbb cdbb.c

clean:
	rm -f ${EXECS}
