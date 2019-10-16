#.
#%matplotlib notebook

import pandas as pd
from IPython.display import display, Markdown
from report import gen_op_md

# # General setup

plot_options = {
    '*.height': 8,
    'client_count.per_worker': False,
}

# # Task set '${md_task_set}', op '${md_op}'

ls_df = pd.read_csv('${pys_ls_df_csv}', index_col=0, parse_dates=True)
zkm_df = pd.read_csv('${pys_zkm_df_csv}', index_col=0, parse_dates=True)

group = gen_op_md.Group('0', None, ls_df, zkm_df)

# ## Summary

ls_merged_df = group.merged_client_stats()

display(Markdown(gen_op_md.gen_summary_md(ls_merged_df, 1)))

# ## Latencies

_ = gen_op_md.LatenciesPlotter(plot_options).plot([group])

# ## Errors

_ = gen_op_md.ErrorsPlotter(plot_options).plot([group])

# ## ZK Client Requests

_ = gen_op_md.RequestFrequencyPlotter(plot_options).plot([group])

# ## ZK Client Count

_ = gen_op_md.ClientCountPlotter(plot_options).plot([group])

# ## ZooKeeper Metrics
#
# ### ZooKeeper Outstanding Requests

zkm_plot_def = gen_op_md.get_zkm_plot_def('outstanding_requests')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot([group])

# ### ZooKeeper Clients

zkm_plot_def = gen_op_md.get_zkm_plot_def('clients')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot([group])

# ### ZooKeeper Nodes

zkm_plot_def = gen_op_md.get_zkm_plot_def('nodes')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot([group])

# ### ZooKeeper Watches

zkm_plot_def = gen_op_md.get_zkm_plot_def('watch_count')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot([group])
