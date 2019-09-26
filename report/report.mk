SCRIPT_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

LOCUST_EXTRA_STATS_CSV = locust-stats.csv
ZK_LOCUST_ZK_METRICS_CSV = zk-metrics.csv

FRAGS_DIR = fragments

$(V).SILENT:

-include $(FRAGS_DIR)/subsets.mk

TASK_SET_EXTRACTS =							\
	$(addprefix $(FRAGS_DIR)/,$(addsuffix				\
		.ls_subset.csv .zkm_subset.csv,$(TASK_SET_OPS)))
TASK_SET_FRAGMENTS = \
	$(addprefix $(FRAGS_DIR)/,$(addsuffix .fragment.done,$(TASK_SET_OPS)))
TASK_SET_MDS = \
	$(addprefix $(FRAGS_DIR)/,$(addsuffix /task_set.md,$(TASK_SETS)))

$(warning TASK_SET_FRAGMENTS $(TASK_SET_FRAGMENTS))
$(warning TASK_SET_MDS $(TASK_SET_MDS))

.PHONY: report
report:						\
		report.html			\
		report.pdf

.PHONY: extracts
extracts: $(TASK_SET_EXTRACTS)

$(FRAGS_DIR)/subsets.mk:			\
		$(LOCUST_EXTRA_STATS_CSV)	\
		$(SCRIPT_DIR)/gen_subsets_mk.py
	@echo '  SUBSETS'
	@mkdir -p $(dir $@)
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
