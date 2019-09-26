SCRIPT_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

LOCUST_EXTRA_STATS_CSV = locust-stats.csv
ZK_LOCUST_ZK_METRICS_CSV = zk-metrics.csv

FRAGS_DIR = fragments
FRAGS_ID =

$(V).SILENT:

-include $(FRAGS_DIR)/subsets.mk

TASK_SET_FRAG_JSONLS = \
	$(addprefix $(FRAGS_DIR)/,$(addsuffix .fragment.jsonl,$(TASK_SET_OPS)))
TASK_SET_FRAG_MDS = \
	$(addprefix $(FRAGS_DIR)/,$(addsuffix .fragment.md,$(TASK_SET_OPS)))
TASK_SET_MDS = \
	$(addprefix $(FRAGS_DIR)/,$(addsuffix /task_set.md,$(TASK_SETS)))

.PHONY: report
report:						\
		report.html			\
		report.pdf

$(FRAGS_DIR)/subsets.mk:			\
		$(LOCUST_EXTRA_STATS_CSV)	\
		$(SCRIPT_DIR)/gen_subsets_mk.py
	@echo '  SUBSETS'
	@mkdir -p $(dir $@)
	$(SCRIPT_DIR)/gen_subsets_mk.py TASK_SETS TASK_SET_OPS $< >$@.tmp
	if cmp -s $@.tmp $@; then rm $@.tmp; else mv $@.tmp $@; fi

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
	@mkdir -p $(dir $@)
	@echo '  EXTRACT  $* ZK metrics'
	$(SCRIPT_DIR)/extract_zkm_subset_csv.py	\
	    $<					\
	    $(ZK_LOCUST_ZK_METRICS_CSV)		\
	    $*					\
	    $@.tmp
	@mv $@.tmp $@

.PRECIOUS: %.fragment.jsonl
%.fragment.jsonl:				\
		%.ls_subset.csv			\
		%.zkm_subset.csv		\
		$(SCRIPT_DIR)/gen_op_info.py
	@echo '  FRAGMENT $*'
	@mkdir -p $(dir $@)
	$(SCRIPT_DIR)/gen_op_info.py		\
	    $*.ls_subset.csv			\
	    $*.zkm_subset.csv			\
	    $*					\
	    $(FRAGS_ID)				\
	    >$@.tmp
	@mv $@.tmp $@

$(FRAGS_DIR)/fragments.jsonl:			\
		$(TASK_SET_FRAG_JSONLS)
	@mkdir -p $(dir $@)
	@echo '  FRAGMENTS'
	cat /dev/null $(TASK_SET_FRAG_JSONLS) >$@.tmp
	@mv $@.tmp $@

.PRECIOUS: %.fragment.md
%.fragment.md:					\
		%.fragment.jsonl		\
		$(SCRIPT_DIR)/gen_op_md.py
	@echo '  FRAGMENT $*'
	@mkdir -p $(dir $@)
	$(SCRIPT_DIR)/gen_op_md.py		\
	    $<					\
	    $(patsubst %.fragment.md,%,$@)	\
	    $@.tmp
	@mv $@.tmp $@

.PRECIOUS: %/task_set.md
%/task_set.md:					\
		$(TASK_SET_FRAG_MDS)
	@echo '  FRAGMENT $*'
	cat /dev/null $(filter $*/%,$(TASK_SET_FRAG_MDS)) >$@.tmp
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
