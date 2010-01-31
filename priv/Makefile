OUTPUT=../ebin/drmaa_drv.so
LDFLAGS=-shared -fPIC -undefined -ldrmaa -lei -lerl_interface -L/usr/lib/erlang/lib/erl_interface-3.6.2/lib 
CFLAGS=-o ${OUTPUT} -I/usr/lib/erlang/usr/include -I/usr/lib/erlang/lib/erl_interface-3.6.2/include/ -Wall -W -Wno-unused-parameter

all:
	gcc drmaa_drv.c ${LDFLAGS} ${CFLAGS} -O2

debug:
	gcc drmaa_drv.c ${LDFLAGS} ${CFLAGS} -ggdb -O0
