"""
Poor man's VCF parsing system.

Built to cover (the various) cases where PyVCF fails.
All VCF parsing logic for the project should live here.
"""
import abc
import re
import vcf.model


class VCFLineParserBase(object):
    # Changed from original to be python 2.7 compatible 
    __metaclass__= abc.ABCMeta
    def __init__(self):
        super(VCFLineParserBase, self).__init__()

    @abc.abstractmethod
    def parse(self, line):
        pass


class MutableRecord(vcf.model._Record):

    def __init__(self, chrom, pos, ref, alt, subj_dict):
        """
        from model.py

        def __init__(self, CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, sample_indexes, samples=None):
        """
        super(MutableRecord, self).__init__(
            chrom, pos, '.', ref, alt, 0.0, '.', '', '', []
        )

        self.subj_dict = subj_dict

    def __str__(self):
        return 'Mutable' + super(MutableRecord, self).__str__()


class SimpleLineParser(VCFLineParserBase):
    _sep = '\t'
    _form_sep = ':'
    _first_subj_field_idx = 9

    _chrom_field = 0
    _pos_field = 1
    _ref_field = 3
    _alt_field = 4
    _format_field = 8

    header_prefix = '#'
    header_kv_sep = '='

    def __int__(self):
        super(SimpleLineParser, self).__init__()

    @staticmethod
    def is_header_field_line(line):
        return line.startswith('#CHROM')

    @staticmethod
    def is_header_line(line):
        return line.startswith(SimpleLineParser.header_prefix)

    @staticmethod
    def header_is_wellformed(line):
        return SimpleLineParser.header_kv_sep in line

    @staticmethod
    def split(line):
        return line.split(SimpleLineParser._sep)

    @staticmethod
    def split_fmt(line):
        return line.split(SimpleLineParser._form_sep)

    @staticmethod
    def chrom(fields):
        return fields[SimpleLineParser._chrom_field]

    @staticmethod
    def pos(fields):
        return int(fields[SimpleLineParser._pos_field])

    @staticmethod
    def ref(fields):
        return fields[SimpleLineParser._ref_field]

    @staticmethod
    def alt(fields):
        return fields[SimpleLineParser._alt_field]

    @staticmethod
    def fmt(fields):
        return fields[SimpleLineParser._format_field]

    @staticmethod
    def subjects(fields):
        return [
            fields[i]
            for i in range(SimpleLineParser._first_subj_field_idx, len(fields))
        ]

    @staticmethod
    def subj_dict(fields):
        format_str = SimpleLineParser.fmt(fields)
        subjects = SimpleLineParser.subjects(fields)

        fmt_fields = SimpleLineParser.split_fmt(format_str)

        subs_dict = {}
        for i, subject_str in enumerate(subjects):
            subject_fields = SimpleLineParser.split_fmt(subject_str)
            if len(fmt_fields) != len(subject_fields):
                raise ValueError(
                    'FORMAT and Subject lists of different size {}!={} for {}, {}.\n'.format(
                        len(fmt_fields), len(subject_fields),
                        fmt_fields, subject_fields
                    )
                )
            subs_dict[i] = dict(list(zip(fmt_fields, subject_fields)))

        return subs_dict

    def parse(self, line):
        """
        Assumes fields 10 - N are subject fields
        """

        fields = SimpleLineParser.split(line)

        return MutableRecord(
            SimpleLineParser.chrom(fields),
            SimpleLineParser.pos(fields),
            SimpleLineParser.ref(fields),
            SimpleLineParser.alt(fields),
            SimpleLineParser.subj_dict(fields)
        )


class SubjectAwareSimpleLineParser(object):

    def __init__(self, subject_list, header_fields_line):
        super(SubjectAwareSimpleLineParser, self).__init__()

        self.subject_list = subject_list
        # find indexes
        self.subject_idxs = self.idx_for_subs(header_fields_line)

    def idx_for_subs(self, header_fields_line):
        # print 'RESOLVING Subjects: \n{}'.format(self.subject_list)
        hfields = header_fields_line.split('\t')
        idxs = [hfields.index(subject) for subject in self.subject_list]
        # print 'Fields: {}'.format(list(enumerate(hfields)))
        # print 'Idxs: {}'.format(idxs)
        return idxs

    def subjects(self, fields):
        return [
            fields[i]
            # for i in range(
            #     SimpleLineParser._first_subj_field_idx,
            #     SimpleLineParser._first_subj_field_idx + len(self.subject_list)
            # )
            for i in self.subject_idxs
        ]

    def subj_dict(self, fields):
        format_str = SimpleLineParser.fmt(fields)
        subjects = self.subjects(fields)

        # print subjects

        fmt_fields = SimpleLineParser.split_fmt(format_str)

        subs_dict = {}
        for i, subject_str in enumerate(subjects):
            subject_fields = SimpleLineParser.split_fmt(subject_str)
            if len(fmt_fields) != len(subject_fields):
                raise ValueError(
                    'FORMAT and Subject lists of different size {}!={} for {}, {}.\n'.format(
                        len(fmt_fields), len(subject_fields),
                        fmt_fields, subject_fields
                    )
                )
            subs_dict[i] = dict(list(zip(fmt_fields, subject_fields)))

        # print subs_dict
        # print

        return subs_dict

    def parse(self, line):
        """
        Assumes fields 10 - N are subject fields
        """

        fields = SimpleLineParser.split(line)

        return MutableRecord(
            SimpleLineParser.chrom(fields),
            SimpleLineParser.pos(fields),
            SimpleLineParser.ref(fields),
            SimpleLineParser.alt(fields),
            self.subj_dict(fields)
        )

def test_simple_line_parser(filename):
    parser = SimpleLineParser()
    with open(filename, 'r') as vcf_fh:
        for line in vcf_fh:
            if re.match('^[^#]', line):
                rec = parser.parse(line)
                for idx, sub in rec.subj_dict.items():
                    print('\t'.join(
                        map(
                            str,
                            [
                                idx, rec.CHROM, rec.POS, rec.REF, rec.ALT,
                                sub['GT'], sub['DQ'], sub['DGQ']
                            ]
                        )
                    ))

if __name__ == '__main__':
    test_simple_line_parser('resources/triodenovo.out.vcf')
