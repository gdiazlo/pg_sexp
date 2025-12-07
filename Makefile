EXTENSION = pg_sexp
MODULE_big = pg_sexp
OBJS = src/pg_sexp.o src/sexp_parser.o src/sexp_io.o src/sexp_ops.o src/sexp_match.o src/sexp_gin.o

DATA = sql/pg_sexp--0.1.0.sql
REGRESS = pg_sexp

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Additional compiler flags
# -O3 for maximum optimization in release builds
# PostgreSQL's PGXS may override with debug flags when USE_ASSERT_CHECKING is set
PG_CFLAGS += -Wall -Wextra -Werror -std=c99 -O3 -fno-strict-aliasing

# Container-based build and test targets
.PHONY: container-build container-test container-shell filc-build filc-test filc-shell
.PHONY: asan-build asan-test asan-stress asan-shell valgrind-build valgrind-test valgrind-shell

.PHONY: production-build production-test production-shell production-run

CONTAINER_ENGINE ?= podman
BUILD_IMAGE = pg_sexp-build
TEST_IMAGE = pg_sexp-test
FILC_IMAGE = pg_sexp-filc
ASAN_IMAGE = pg_sexp-asan
VALGRIND_IMAGE = pg_sexp-valgrind

PRODUCTION_IMAGE = pg_sexp-production

container-build:
	$(CONTAINER_ENGINE) build -f Containerfile.build -t $(BUILD_IMAGE) .
	$(CONTAINER_ENGINE) run --rm -v $$(pwd):/src:Z $(BUILD_IMAGE)

container-test:
	$(CONTAINER_ENGINE) build -f Containerfile.test -t $(TEST_IMAGE) .
	$(CONTAINER_ENGINE) run --rm $(TEST_IMAGE)

container-shell:
	$(CONTAINER_ENGINE) run --rm -it $(TEST_IMAGE) bash

# Fil-C memory-safe build targets
# Note: Fil-C cannot be used for PostgreSQL extensions that link against
# standard PostgreSQL due to symbol transformation. Keep for future use
# when building PostgreSQL itself with fil-c.
filc-build:
	$(CONTAINER_ENGINE) build -f Containerfile.filc -t $(FILC_IMAGE) .

filc-test: filc-build
	$(CONTAINER_ENGINE) run --rm $(FILC_IMAGE)

filc-shell:
	$(CONTAINER_ENGINE) run --rm -it $(FILC_IMAGE) /bin/bash

# AddressSanitizer (ASan) memory safety testing
# Detects buffer overflows, use-after-free, memory leaks
asan-build:
	$(CONTAINER_ENGINE) build -f Containerfile.asan -t $(ASAN_IMAGE) .

asan-test: asan-build
	$(CONTAINER_ENGINE) run --rm $(ASAN_IMAGE)

asan-stress: asan-build
	$(CONTAINER_ENGINE) run --rm $(ASAN_IMAGE) /run-stress-test.sh

asan-shell:
	$(CONTAINER_ENGINE) run --rm -it $(ASAN_IMAGE) bash

# Valgrind memory checking
# Thorough but slow (~10-20x overhead), good for CI/nightly testing
valgrind-build:
	$(CONTAINER_ENGINE) build -f Containerfile.valgrind -t $(VALGRIND_IMAGE) .

valgrind-test: valgrind-build
	$(CONTAINER_ENGINE) run --rm $(VALGRIND_IMAGE)

valgrind-full: valgrind-build
	$(CONTAINER_ENGINE) run --rm $(VALGRIND_IMAGE) /run-full-tests.sh

valgrind-shell:
	$(CONTAINER_ENGINE) run --rm -it $(VALGRIND_IMAGE) bash

# Run all memory safety tests
memory-test: asan-test valgrind-test
	@echo "All memory safety tests passed!"

# Production container with PostgreSQL 18.1, TimescaleDB, and pg_sexp
# Designed for production use with distributed TimescaleDB clusters
production-build:
	$(CONTAINER_ENGINE) build -f Containerfile.production -t $(PRODUCTION_IMAGE) .

production-test: production-build
	$(CONTAINER_ENGINE) run --rm $(PRODUCTION_IMAGE) bash -c '\
		gosu postgres initdb -D /var/lib/postgresql/data --auth=trust --no-locale --encoding=UTF8 && \
		echo "shared_preload_libraries = '\''timescaledb'\''" >> /var/lib/postgresql/data/postgresql.conf && \
		echo "listen_addresses = '\'''\''" >> /var/lib/postgresql/data/postgresql.conf && \
		gosu postgres pg_ctl -D /var/lib/postgresql/data start && \
		sleep 2 && \
		gosu postgres psql -c "CREATE EXTENSION timescaledb;" && \
		gosu postgres psql -c "CREATE EXTENSION pg_sexp;" && \
		gosu postgres psql -c "SELECT version();" && \
		gosu postgres psql -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('\''timescaledb'\'', '\''pg_sexp'\'');" && \
		echo "Production container test passed!"'

production-shell:
	$(CONTAINER_ENGINE) run --rm -it $(PRODUCTION_IMAGE) bash

# Run production container as a daemon (for actual use)
# Note: This exposes port 5432, ensure no conflicts with other PostgreSQL instances
production-run:
	$(CONTAINER_ENGINE) run -d --name pg_sexp-prod \
		-v pg_sexp_data:/var/lib/postgresql/data \
		-p 5432:5432 \
		$(PRODUCTION_IMAGE)
	@echo "Production container started. Connect with: psql -h localhost -U postgres"
	@echo "Stop with: $(CONTAINER_ENGINE) stop pg_sexp-prod && $(CONTAINER_ENGINE) rm pg_sexp-prod"

production-stop:
	$(CONTAINER_ENGINE) stop pg_sexp-prod || true
	$(CONTAINER_ENGINE) rm pg_sexp-prod || true
