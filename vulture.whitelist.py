from rkstr8.cloud import Service
from rkstr8.cloud.batch import BatchJobListStatusPoller
from handlers.alignment_polling import handler as alignment_polling_handler
from handlers.alignment_processing import handler as alignment_processing_handler
from handlers.sentieon_genotyper import handler as genotyper_handler
from handlers.sentieon_haplotyper import handler as haplotyper_handler

Service._generate_next_value_(
    name='test',
    start=0,
    count=1,
    last_values=0
)

BatchJobListStatusPoller(job_ids=[])

alignment_polling_handler(
    event=None,
    context=None
)

alignment_processing_handler(
    event=None,
    context=None
)

genotyper_handler(
    event=None,
    context=None
)

haplotyper_handler(
    event=None,
    context=None
)
