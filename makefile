EXECS=cdbb cdbb-dedupe ldbb nobb dedupe
MPICC=mpicc

all: ${EXECS}

cdbb: cdbb.c
	${MPICC} -o cdbb cdbb.c
 
cdbb-dedupe: cdbb-dedupe.c
	${MPICC} -o cdbb-dedupe -lssl cdbb-dedupe.c
 
dedupe: dedupe.c
	${MPICC} -o dedupe -lssl dedupe.c
	
nobb: nobb.c
	${MPICC} -o nobb nobb.c

ldbb: ldbb.c
	${MPICC} -o ldbb ldbb.c

clean:
	rm -f ${EXECS}
