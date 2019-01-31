import sys


def proc_subs(subs):
    found_mom = False
    found_dad = False
    found_pup = False

    mom_i = -1
    dad_i = -1
    pup_i = -1

    for i, sub in enumerate(subs):
        if sub.startswith('D'):
            found_dad = True
            dad_i = i
        if sub.startswith('M'):
            found_mom = True
            mom_i = i
        if sub.startswith('F'):
            found_pup = True
            pup_i = i

    return {
        'good': all((
            found_mom,
            found_dad,
            found_pup
        )), 
        'indices': {
            'mom': mom_i,
            'dad': dad_i,
            'pup': pup_i
        }
    }


def fam_id_from_sub(sub):
    return sub[1:]


def sub_from_data(member_id, subs, subs_data):
    return subs[subs_data['indices'][member_id]]


def main(vcf):
    with open (vcf, 'r') as f:
        for line in f:
            if line.startswith('#CHROM'):
                scrubbed_line = line.strip()
                fields = scrubbed_line.split('\t')
                subs = fields[-3:]
                subs_data = proc_subs(subs)
                if subs_data['good']:
                    fam_id = fam_id_from_sub(sub_from_data('dad',subs,subs_data))
                    print('\t'.join((fam_id, subs[subs_data['indices']['dad']], '0', '0', '1')))
                    print('\t'.join((fam_id, subs[subs_data['indices']['mom']], '0', '0', '2')))
                    print('\t'.join((
                        fam_id, 
                        subs[subs_data['indices']['pup']], 
                        subs[subs_data['indices']['dad']], 
                        subs[subs_data['indices']['mom']], 
                        '2')))
                    sys.exit(0)

            if not line.startswith('#'):
                sys.stderr.write('Scanned header and could not find column defintion line!' + '\n')
                sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1])
