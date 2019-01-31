"""
Hail script that validates the pipeline by comparing a pipeline-run version of NA12878 (GiAB) sample
with the gold standard GiAB VCF.
"""

import hail as hl
from google.cloud import storage
import sys
import datetime
import os
import subprocess


def split_multi(ds):
    sm = hl.SplitMulti(ds)
    sm.update_rows(a_index=sm.a_index(), was_split=sm.was_split())
    sm.update_entries(
         GT=hl.downcode(ds.GT, sm.a_index()),
         AD=hl.or_missing(hl.is_defined(ds.AD),
                         [hl.sum(ds.AD) - ds.AD[sm.a_index()], ds.AD[sm.a_index()]]),
         DP=ds.DP
    )
    split_ds = sm.result()
    return split_ds

print("arguments", sys.argv)

#Arguments from cloudspan lambda
bucket_name = sys.argv[1]
pipeline_run_vcf = sys.argv[2]
cohort_prefix = sys.argv[3]
reference_build = sys.argv[4]
giab_bucket = sys.argv[5]

# Load GiAB VCF, split multi-allelic sites, and store as MatrixTable
build_37 = ["GRCh37", "37", "hg19"]

if reference_build in build_37:
	reference = hl.get_reference('GRCh37')
else:
	reference = hl.get_reference('GRCh38')

giab_gs_path = '{}/HG001_GRCh38_GIAB_highconf_CG-IllFB-IllGATKHC-Ion-10X-SOLID_CHROM1-X_v.3.3.2_highconf_PGandRTGphasetransfer.vcf.bgz'.format(giab_bucket)
giab_ds = hl.import_vcf(path=giab_gs_path, reference_genome=reference)
giab_ds = split_multi(giab_ds)
giab_ds.describe()

# Load pipeline-run VCF, split multi-allelic sites, and store as MatrixTable

pipeline_gs_path = '{}{}'.format(bucket_name, pipeline_run_vcf)
pipeline_ds = hl.import_vcf(path=pipeline_gs_path, reference_genome=reference)
pipeline_ds = split_multi(pipeline_ds)
pipeline_ds.describe()


autosomes_interval_expressions = []
for chrom in range(1,23):
    interval_expr = hl.parse_locus_interval('chr' + str(chrom), reference_genome=reference)
    autosomes_interval_expressions.append(interval_expr)

giab_auts_ds = hl.filter_intervals(giab_ds, autosomes_interval_expressions)
pipeline_auts_ds = hl.filter_intervals(pipeline_ds, autosomes_interval_expressions)

giab_auts_ds = giab_auts_ds.annotate_entries(GT = hl.call(giab_auts_ds.GT[0], giab_auts_ds.GT[1], phased=False))
pipeline_auts_ds = pipeline_auts_ds.annotate_entries(GT = hl.call(pipeline_auts_ds.GT[0], pipeline_auts_ds.GT[1], phased=False))

# Run genotype concordance
global_conc, cols_conc, rows_conc = hl.concordance(giab_auts_ds, pipeline_auts_ds)

summary = global_conc

left_homref_right_homvar = summary[2][4]
left_het_right_missing = summary[3][1]
left_het_right_something_else = sum(summary[3][:]) - summary[3][3]
total_concordant = summary[2][2] + summary[3][3] + summary[4][4]
total_discordant = sum([sum(s[2:]) for s in summary[2:]]) - total_concordant

concordance = total_concordant/float(total_concordant + total_discordant)

now = datetime.datetime.utcnow()

results_bucket = "{}/validation/{}/validation-result-{}{}{}.txt".format(bucket_name, cohort_prefix, now.month, now.day, now.year)

with hl.hadoop_open(results_bucket, 'w') as out:
	out.write("% Concordance:")
	out.write(str(concordance))