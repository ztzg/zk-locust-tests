# TODO(ddiederen): connect doesn't play well with greenlet exists.
TESTS =						\
	delete					\
	exists_many				\
	exists					\
	get_children2				\
	get_children				\
	get					\
	set_and_get				\
	set					\
	watch

OUT = out
TMP = $(OUT)/tmp

MULTI_COUNT = 0

NUM_CLIENTS = 128
HATCH_RATE = 32
RUN_TIME = 60s

.PHONY: run
run: $(addprefix $(OUT)/,$(addsuffix .log,$(TESTS)))

.PRECIOUS: $(OUT)/%.log
$(OUT)/%.log:					\
		locust_%.py			\
		common.py			\
		multi-locust.sh
	@mkdir -p $(TMP)/$* $(OUT)
	bash multi-locust.sh $(MULTI_COUNT) $(TMP)/$*			\
	    --no-web -c $(NUM_CLIENTS) -r $(HATCH_RATE) -t $(RUN_TIME)	\
	    --csv=$(OUT)/$* --reset-stats				\
	    -f $< 2>&1 | tee $@
