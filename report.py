#!/usr/bin/env python3

import os
import os.path

import click

_base = os.path.dirname(os.path.realpath(__file__))


@click.command()
@click.option("--metrics-dir", help="Directory containing collected metrics")
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
    if not metrics_dir:
        metrics_dir = "."
    if not zk_metrics_csv:
        zk_metrics_csv = "%s/zk-metrics.csv" % metrics_dir
    if not stats_csv:
        stats_csv = "%s/locust-stats.csv" % metrics_dir

    no_access = []
    for f in [zk_metrics_csv, stats_csv]:
        if not (os.path.isfile(f) and os.access(f, os.R_OK)):
            no_access.append("'%s'" % f)
    if len(no_access):
        raise click.ClickException(
            ("Cannot locate %s; please specify location using --metrics-dir " +
             "or individual flags.") % " nor ".join(no_access))

    is_in_place = False
    if not report_dir:
        if in_place:
            report_dir = metrics_dir
            is_in_place = True
        else:
            raise click.ClickException(
                "Please specify --report-dir or --in-place.")

    if os.path.exists(report_dir) and not (is_in_place or force):
        raise click.ClickException(
            ("Refusing to touch existing report path '%s'; " +
             "please remove or use --force.") % report_dir)

    report_mk = os.path.join(_base, "report", "report.mk")

    make_args = [
        "make", "--no-print-directory", "-C", report_dir, "-f", report_mk,
        "LOCUST_EXTRA_STATS_CSV=" + os.path.abspath(stats_csv),
        "ZK_LOCUST_ZK_METRICS_CSV=" + os.path.abspath(zk_metrics_csv)
    ]

    if jobs:
        make_args += ["-j", str(jobs)]
    if verbose:
        make_args.append("V=1")

    os.makedirs(report_dir, exist_ok=True)
    os.execvp("make", make_args + ["report"])


if __name__ == "__main__":
    cli()
