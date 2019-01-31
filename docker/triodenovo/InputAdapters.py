from VCFLineParser import SimpleLineParser
import sys
import argparse


class TrioDeNovoInputAdapter(object):
    """
    This should really extend VCFOutputAdapter or something similar
    """
    def __init__(self, vcf, output_handle=sys.stdout):
        self.vcf = vcf
        self.output_handle = output_handle

    @staticmethod
    def check_format_field(subject_dict, field_name):
        for index, format_str in subject_dict.items():
            try:
                if format_str[field_name] == '.':
                    raise ValueError('{} is .'.format(field_name))
            except KeyError:
                raise ValueError('{} field missing'.format(field_name))

    def is_spanning_del_only(self, line):
        fields = line.split('\t')
        alt = fields[4]
        return alt == '*'

    def parse(self):
        with open(self.vcf, 'r') as vcf_fh:
            for line in vcf_fh:
                stripped_line = line.strip()
                if SimpleLineParser.is_header_line(stripped_line):
                    self.output_handle.write(line)
                if not SimpleLineParser.is_header_line(stripped_line):
                    # not a header line
                    try:
                        # raises ValueError if number FORMAT fields doesn't match number of subject fields
                        subj_dict = SimpleLineParser.subj_dict(
                            SimpleLineParser.split(stripped_line)
                        )

                        # raises ValueError if any subject PL is '.'
                        TrioDeNovoInputAdapter.check_format_field(subj_dict, 'PL')
                        TrioDeNovoInputAdapter.check_format_field(subj_dict, 'DP')

                        # if did not throw a value error, write line
                        if not self.is_spanning_del_only(stripped_line):
                            self.output_handle.write(line)
                    except ValueError as ve:
                        # Skip lines with mis-matched FORMAT fields
                        # sys.stderr.write(str(ve) + '\n')
                        # sys.stderr.write(line)
                        continue

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='vcf', required=True)
    parser.add_argument('-o', dest='out', required=False, type=argparse.FileType('w'), default=sys.stdout)
    args = parser.parse_args()

    TrioDeNovoInputAdapter(args.vcf, args.out).parse()

    if args.out != sys.stdout:
        print('Closing ' + str(args.out))
        args.out.close()
