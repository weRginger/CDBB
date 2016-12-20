EXECS=cdbb ldbb nobb dedupe
MPICC=mpicc

all: ${EXECS}

cdbb: cdbb.c
	${MPICC} -o cdbb cdbb.c
 
dedupe: dedupe.c
	${MPICC} -o dedupe -lssl dedupe.c
	
nobb: nobb.c
	${MPICC} -o nobb nobb.c

ldbb: ldbb.c
	${MPICC} -o ldbb ldbb.c

clean:
	rm -f ${EXECS}
