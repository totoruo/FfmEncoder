import hashlib, math, os, subprocess
from multiprocessing import Process


def hashstr(str, nr_bins=1e+6):
    return int(hashlib.md5(str.encode('utf8')).hexdigest(), 16) % (int(nr_bins) - 1) + 1


class FfmEncoder():
    def __init__(self, field_names, label_name, nthread=1):
        self.field_names = field_names
        self.nthread = nthread
        self.label = label_name

    def gen_feats(self, row):
        feats = []
        for field in self.field_names:
            value = row[field]
            key = field + '-' + str(value)
            feats.append(key)
        return feats

    def gen_hashed_fm_feats(self, feats):
        feats = ['{0}:{1}:1'.format(field, hashstr(feat, 1e+6)) for (field, feat) in feats]
        return feats

    def convert(self, df, path, i):
        lines_per_thread = math.ceil(float(df.shape[0]) / self.nthread)
        sub_df = df.iloc[i * lines_per_thread: (i + 1) * lines_per_thread]
        tmp_path = path + '_tmp_{0}'.format(i)
        with open(tmp_path, 'w') as f:
            for index,row in sub_df.iterrows():
                feats = []
                for i, feat in enumerate(self.gen_feats(row)):
                    feats.append((i, feat))
                feats = self.gen_hashed_fm_feats(feats)
                f.write(str(int(row[self.label])) + ' ' + ' '.join(feats) + '\n')

    def parallel_convert(self, df, path):
        processes = []
        for i in range(self.nthread):
            p = Process(target=self.convert, args=(df, path, i))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

    def delete(self, path):
        for i in range(self.nthread):
            os.remove(path + '_tmp_{0}'.format(i))

    def cat(self, path):
        if os.path.exists(path):
            os.remove(path)
        for i in range(self.nthread):
            cmd = 'cat {svm}_tmp_{idx} >> {svm}'.format(svm=path, idx=i)
            p = subprocess.Popen(cmd, shell=True)
            p.communicate()

    def transform(self, df, path):
        print('converting data......')
        self.parallel_convert(df, path)
        self.cat(path)
        self.delete(path)
