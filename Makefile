EXTENSION = x5fix
MODULES = x5fix
DATA = x5fix--0.1.sql
OBJS = x5fix.o

MODULE_big = x5fix
PG_CPPFLAGS = -g -O2

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)