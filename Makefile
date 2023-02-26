INST_PREFIX ?= /usr
INST_LIBDIR ?= $(INST_PREFIX)/lib/lua/5.1
INST_LUADIR ?= $(INST_PREFIX)/share/lua/5.1
INSTALL ?= install

.PHONY: build
build:
	go build -buildmode=c-shared -o libresty_ffi_kafka.so kafka.go

.PHONY: install
install:
	$(INSTALL) -d $(INST_LUADIR)/resty/ffi_kafka
	$(INSTALL) lib/resty/ffi_kafka/*.lua $(INST_LUADIR)/resty/ffi_kafka
	$(INSTALL) -d $(INST_LIBDIR)/
	$(INSTALL) libffi_go_kafka.so $(INST_LIBDIR)/
