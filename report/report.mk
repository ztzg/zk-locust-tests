SCRIPT_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

LOCUST_EXTRA_STATS_CSV = locust-stats.csv
ZK_LOCUST_ZK_METRICS_CSV = zk-metrics.csv

$(V).SILENT:

-include subsets.mk

TASK_SET_FRAGMENTS = $(addsuffix .fragment.done,$(TASK_SET_OPS))
TASK_SET_MDS = $(addsuffix /task_set.md,$(TASK_SETS))

.PHONY: report
report:						\
		report.html			\
		report.pdf

subsets.mk:					\
		$(LOCUST_EXTRA_STATS_CSV)	\
		$(SCRIPT_DIR)/gen_subsets_mk.py
	@echo '  SUBSETS'
	$(SCRIPT_DIR)/gen_subsets_mk.py TASK_SETS TASK_SET_OPS $< >$@.tmp
	@mv $@.tmp $@

.PRECIOUS: %.ls_subset.csv
%.ls_subset.csv:					\
		$(LOCUST_EXTRA_STATS_CSV)		\
		$(SCRIPT_DIR)/extract_ls_subset_csv.py
	@echo '  EXTRACT  $* Locust stats'
	@mkdir -p $(dir $@)
	$(SCRIPT_DIR)/extract_ls_subset_csv.py	\
	    $(LOCUST_EXTRA_STATS_CSV)		\
	    $*					\
	    $@.tmp
	@mv $@.tmp $@

.PRECIOUS: %.zkm_subset.csv
%.zkm_subset.csv:					\
		%.ls_subset.csv				\
		$(ZK_LOCUST_ZK_METRICS_CSV)		\
		$(SCRIPT_DIR)/extract_zkm_subset_csv.py
	@echo '  EXTRACT  $* ZK metrics'
	$(SCRIPT_DIR)/extract_zkm_subset_csv.py	\
	    $<					\
	    $(ZK_LOCUST_ZK_METRICS_CSV)		\
	    $*					\
	    $@.tmp
	@mv $@.tmp $@

.PRECIOUS: %.fragment.done
%.fragment.done:				\
		%.ls_subset.csv			\
		%.zkm_subset.csv		\
		$(SCRIPT_DIR)/gen_op_md.py
	@echo '  FRAGMENT $*'
	@mkdir -p $(dir $@)
	$(SCRIPT_DIR)/gen_op_md.py		\
	    $*.ls_subset.csv			\
	    $*.zkm_subset.csv			\
	    $*					\
	    $*
	@touch $@

.PRECIOUS: %/task_set.md
%/task_set.md:					\
		$(TASK_SET_FRAGMENTS)
	@echo '  FRAGMENT $*'
	cat /dev/null $(patsubst %.fragment.done,%.md,$(filter $*/%,$(TASK_SET_FRAGMENTS))) >$@.tmp
	@mv $@.tmp $@

report.md: $(TASK_SET_MDS)
	@echo '  REPORT   $@'
	echo '# Report' >$@.tmp
	echo >>$@.tmp
	cat /dev/null $(TASK_SET_MDS) >>$@.tmp
	@mv $@.tmp $@

report.html: report.md
	@echo '  REPORT   $@'
	pandoc --default-image-extension=svg -o $@ $<

report.pdf: report.md
	@echo '  REPORT   $@'
	pandoc --default-image-extension=pdf -o $@ $<
