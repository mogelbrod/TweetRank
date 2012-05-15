from OsUtils import safemkdir, safemv, generate_tmp_fname
import os

class Backup:
    def __init__(self, datadir):
        self.backupdir = datadir + '/backup/'
        self.max_size  = 16*1024*1024; # 32MB per file
        safemkdir(self.backupdir)

    def getLastVersion(self, prefix):
        from glob import glob
        version = None
        for f in glob('%s.*' % prefix):
            p = f.split('.')
            v = int(p[-1])
            if version == None or version < v: version = v
        return version

    def prepare_work(self, current_name):
        working_name = generate_tmp_fname(current_name)
        wf = open(working_name, 'w')
        wf.write('<?xml version="1.0" encoding="UTF-8"?>\n');
        wf.write('<statuses type="array">\n');
        return working_name, wf

    def copy_current(self, current_name, wf):
        ignore_lines = ['<?xml version="1.0" encoding="utf-8"?>', '<statuses type="array">', '</statuses>']
        rf = open(current_name, 'r')
        for line in rf:
            line = line.strip()
            if line.lower() in ignore_lines: continue
            else: wf.write(line + '\n')
        rf.close()

    def add_xml(self, xml, wf):
        lxml = xml.split('\n')
        for i in range(2, len(lxml)-1):
            ignore_lines = ['<?xml version="1.0" encoding="utf-8"?>', '<statuses type="array">', '</statuses>']
            line = lxml[i].strip()
            if line.lower() in ignore_lines: continue
            else: wf.write(line + '\n')

    def finish_work(self, current_name, working_name, wf):
        wf.write('</statuses>')
        wf.close()
        safemv(working_name, current_name)


    def store_tweets(self, user_id, xml):
        if isinstance(xml, unicode):
            xml = xml.encode('utf-8')

        prefix = '%s/%d.tweets' % (self.backupdir, user_id)
        version = self.getLastVersion(prefix)
        if version > 0:
            current_name = '%s.%d' % (prefix, version)
            stat = os.stat(current_name)
            if stat.st_size + len(xml) > self.max_size:
                current_name = '%s.%d' % (prefix, version+1)
                working_name, working_file = self.prepare_work(current_name)
            else:
                working_name, working_file = self.prepare_work(current_name)
                self.copy_current(current_name, working_file)
        else:
            current_name = '%s.%d' % (prefix, 1)
            working_name, working_file = self.prepare_work(current_name)

        self.add_xml(xml, working_file)
        self.finish_work(current_name, working_name, working_file)

