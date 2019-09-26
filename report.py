#!/usr/bin/env python3

import sys
import os
import os.path
import subprocess
import json

import click

_base = os.path.dirname(os.path.realpath(__file__))
_report_scripts = os.path.join(_base, 'report')

sys.path.append(_report_scripts)

if True:
    import gen_op_md


@click.command()
@click.option(
    '--metrics-dir',
    help='Directory containing collected metrics',
    multiple=True)
@click.option("--zk-metrics-csv", help="Collected ZooKeeper metrics")
@click.option("--stats-csv", help="Collected Locusts metrics")
@click.option("--report-dir", help="Target directory for report")
@click.option(
    "--in-place",
    is_flag=True,
    help="Generate/update report in metrics directory")
@click.option("-f", "--force", is_flag=True, help="Possibly overwrite files")
@click.option("-j", "--jobs", type=click.INT, help="Use parallel jobs")
@click.option('-v', '--verbose', count=True)
def cli(metrics_dir, zk_metrics_csv, stats_csv, report_dir, in_place, force,
        jobs, verbose):
    if len(metrics_dir) == 0:
        metrics_dir = ['.']

    is_multi = len(metrics_dir) > 1

    if is_multi and (zk_metrics_csv or stats_csv):
        raise click.ClickException(
            '--zk-metrics-csv and --stats-csv can only be used for ' +
            'single-dataset reports.')

    zk_metrics_csvs = [
        zk_metrics_csv or '%s/zk-metrics.csv' % x for x in metrics_dir
    ]
    stats_csvs = [stats_csv or '%s/locust-stats.csv' % x for x in metrics_dir]

    no_access = []
    for f in zk_metrics_csvs + stats_csvs:
        if not (os.path.isfile(f) and os.access(f, os.R_OK)):
            no_access.append("'%s'" % f)
    if len(no_access):
        raise click.ClickException(
            ("Cannot locate '%s'; please specify locations using " +
             "--metrics-dir or individual flags.") % "', '".join(no_access))

    is_in_place = False
    if not report_dir:
        if is_multi:
            raise click.ClickException(
                '--report-dir is required for multi-dataset reports.')
        elif in_place:
            report_dir = metrics_dir[0]
            is_in_place = True
        else:
            raise click.ClickException(
                'Please specify --report-dir or --in-place.')

    if os.path.exists(report_dir) and not (is_in_place or force):
        raise click.ClickException(
            ("Refusing to touch existing report dir '%s'; " +
             "please remove or use --force.") % report_dir)

    report_mk = os.path.join(_report_scripts, 'report.mk')

    make_args = [
        'make',
        '--no-print-directory',
        '-C',
        report_dir,
        '-f',
        report_mk,
    ]

    if jobs:
        make_args += ['-j', str(jobs)]
    if verbose:
        make_args.append('V=1')

    os.makedirs(report_dir, exist_ok=True)

    # Single-dataset reports can be done in one shot.
    if not is_multi:
        extra_args = [
            'LOCUST_EXTRA_STATS_CSV=' + os.path.abspath(stats_csvs[0]),
            'ZK_LOCUST_ZK_METRICS_CSV=' + os.path.abspath(zk_metrics_csvs[0]),
            'report'
        ]
        os.execvp(make_args[0], make_args + extra_args)
        return  # But execvp should have taken over.

    fragments = []
    for i in range(len(metrics_dir)):
        frags_id = str(i)
        frags_dir = os.path.join('fragments', frags_id)
        frags_target = os.path.join(frags_dir, 'fragments.jsonl')
        frags_path = os.path.join(report_dir, frags_target)

        extra_args = [
            'FRAGS_ID=' + frags_id, 'FRAGS_DIR=' + frags_dir,
            'LOCUST_EXTRA_STATS_CSV=' + os.path.abspath(stats_csvs[i]),
            'ZK_LOCUST_ZK_METRICS_CSV=' + os.path.abspath(zk_metrics_csvs[i]),
            frags_target
        ]
        r = subprocess.call(make_args + extra_args)
        if r != 0:
            raise click.ClickException("Failed to generate '%s'." % frags_path)

        with open(frags_path) as f:
            lines = f.readlines()

        fragments += [json.loads(line) for line in lines]

    top_frags_dir = os.path.join(report_dir, 'fragments')
    gen_op_md.process_fragments(report_dir, fragments, top_frags_dir,
                                os.path.join(top_frags_dir, 'report.md'))


if __name__ == "__main__":
    cli()
