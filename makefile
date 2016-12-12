EXECS=cdbb ldbb nobb sha1
MPICC=mpicc

all: ${EXECS}

cdbb: cdbb.c
	${MPICC} -o cdbb cdbb.c
 
sha1: sha1.c
	${MPICC} -o sha1 -lssl sha1.c
	
nobb: nobb.c
	${MPICC} -o nobb nobb.c

ldbb: ldbb.c
	${MPICC} -o ldbb ldbb.c

clean:
	rm -f ${EXECS}
