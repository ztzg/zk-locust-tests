#.
#%config InlineBackend.figure_formats = ['svg']
#%matplotlib notebook

import json
import pandas as pd
from IPython.display import display, Markdown
from report import gen_op_md

# # General setup

plot_options = {'*.height': 8, '*.shade': False}

# # Task set '${md_task_set}', op '${md_op}'

groups_data_json = """
${py_groups_data_json}
"""

groups_data = json.loads(groups_data_json)
groups = [gen_op_md.load_group('.', group_data) for group_data in groups_data]

# ## Latencies

_ = gen_op_md.LatenciesPlotter(plot_options).plot(groups)

# ## Errors

_ = gen_op_md.ErrorsPlotter(plot_options).plot(groups)

# ## ZK Client Requests

_ = gen_op_md.RequestFrequencyPlotter(plot_options).plot(groups)

# ## ZK Client Count

_ = gen_op_md.ClientCountPlotter(plot_options).plot(groups)

# ## ZooKeeper Metrics
#
# ### ZooKeeper Outstanding Requests

zkm_plot_def = gen_op_md.get_zkm_plot_def('outstanding_requests')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot(groups)

# ### ZooKeeper Clients

zkm_plot_def = gen_op_md.get_zkm_plot_def('clients')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot(groups)

# ### ZooKeeper Nodes

zkm_plot_def = gen_op_md.get_zkm_plot_def('nodes')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot(groups)

# ### ZooKeeper Watches

zkm_plot_def = gen_op_md.get_zkm_plot_def('watch_count')
_ = gen_op_md.ZooKeeperMetricsPlotter(zkm_plot_def, plot_options).plot(groups)
