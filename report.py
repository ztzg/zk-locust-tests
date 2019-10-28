#!/usr/bin/env python3

import sys
import os
import os.path
import subprocess
import json

import shutil

import click

_base = os.path.dirname(os.path.realpath(__file__))
_report_scripts = os.path.join(_base, 'report')

sys.path.append(_report_scripts)

if True:
    import gen_op_md


def _has_pandoc():
    return shutil.which("pandoc") is not None


@click.command()
@click.option(
    '--metrics-dir',
    multiple=True,
    help='Directory containing collected metrics')
@click.option(
    "--labeled-metrics-dir",
    type=(str, str),
    multiple=True,
    help="Like --metrics-dir, but also defines a label")
@click.option("--zk-metrics-csv", help="Collected ZooKeeper metrics")
@click.option("--stats-csv", help="Collected Locusts metrics")
@click.option("--report-dir", help="Target directory for report")
@click.option(
    "--in-place",
    is_flag=True,
    help="Generate/update report in metrics directory")
@click.option(
    "--option",
    type=(str, str),
    multiple=True,
    help="Set named report or plot option")
@click.option(
    "--md/--no-md", default=True, help="Generate Markdown-based report")
@click.option(
    "--pdf/--no-pdf",
    default=_has_pandoc,
    show_default='if Pandoc available',
    help="Generate PDF from Markdown report")
@click.option(
    "--html/--no-html",
    default=_has_pandoc,
    show_default='if Pandoc available',
    help="Generate HTML from Markdown report")
@click.option("--nb/--no-nb", default=False, help="Generate Jupyter notebook")
@click.option("-f", "--force", is_flag=True, help="Possibly overwrite files")
@click.option("-j", "--jobs", type=click.INT, help="Use parallel jobs")
@click.option('-v', '--verbose', count=True)
def cli(metrics_dir, labeled_metrics_dir, zk_metrics_csv, stats_csv,
        report_dir, option, in_place, md, pdf, html, nb, force, jobs, verbose):
    if metrics_dir and labeled_metrics_dir:
        raise click.ClickException(
            '--metrics-dir and --labeled-metrics-dir cannot be used together.')

    labels = None
    if labeled_metrics_dir:
        metrics_dir = [b for (a, b) in labeled_metrics_dir]
        labels = [a for (a, b) in labeled_metrics_dir]

    if not metrics_dir:
        metrics_dir = ['.']

    # "Normalize" to dict.
    options = {t[0]: t[1] for t in option}

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
        'make', '--no-print-directory', '-C', report_dir, '-f', report_mk,
        '--keep-going'
    ]

    if jobs:
        make_args += ['-j', str(jobs)]
    if verbose:
        make_args.append('V=1')

    os.makedirs(report_dir, exist_ok=True)

    if options:
        options_json = os.path.join(report_dir, 'options.json')
        with open(options_json, 'w') as f:
            json.dump(options, f)
        make_args.append('OPTIONS_JSON=' + os.path.abspath(options_json))

    # Single-dataset reports can be done in one shot.
    if not is_multi:
        extra_args = [
            'LOCUST_EXTRA_STATS_CSV=' + os.path.abspath(stats_csvs[0]),
            'ZK_LOCUST_ZK_METRICS_CSV=' + os.path.abspath(zk_metrics_csvs[0]),
            'GEN_MD=' + ('1' if md else ''), 'GEN_PDF=' + ('1' if pdf else ''),
            'GEN_HTML=' + ('1' if html else ''),
            'GEN_NB=' + ('1' if nb else ''), 'report'
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

        for line in lines:
            fragment = json.loads(line)
            if labels and labels[i]:
                for data_item in fragment.get('data', []):
                    if not data_item.get('label'):
                        data_item['label'] = labels[i]
            fragments.append(fragment)

    top_frags_dir = os.path.join(report_dir, 'fragments')

    # TODO: This passes md and nb as flag--and does not pass html/pdf
    # at all because that portion of the pipeline has not been
    # implemented yet.
    gen_op_md.process_fragments(report_dir, fragments, top_frags_dir, 'mix',
                                md, nb, options)


if __name__ == "__main__":
    cli()
