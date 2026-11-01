PYTHON ?= python3

.PHONY: test-fast test-core test-full test-race test-report test-infra

test-fast test-core test-full test-race:
	$(PYTHON) -B scripts/go_test.py run $(@:test-%=%)

test-report:
	$(PYTHON) -B scripts/go_test.py report

test-infra:
	$(PYTHON) -B -m unittest discover -s scripts -p 'test_go_test.py' -v
