import sys
import pprint

from client.gal import TroiaClient

TROIA_ADDRESS = 'http://localhost:8080/service'

ALGORITHM = "BDS"

ITERATIONS = 10
APACK_SIZE = 5000


def iter_assigns(assigns_fname):
    with open(assigns_fname, 'r') as F:
        for l in F:
            l = l.strip()
            if l == "":
                break
            spl = l.split(' ')
            worker, object, label = spl[0], spl[1], ' '.join(spl[2:])
            yield worker, object, label


def get_categories(assigns_fname):
    categories = set()
    for _, __, label in iter_assigns(assigns_fname):
        categories.add(label)
    return categories


def create(categories):
    tc = TroiaClient(TROIA_ADDRESS)
    tc.create(categories, algorithm=ALGORITHM, iterations=ITERATIONS)
    return tc


def wc(tc, resp):
    resp = tc.await_completion(resp)
    if resp['status'] != 'OK':
        pprint.pprint(resp)
        assert False
    return resp


def load_assigns(fname):
    i = 0
    w = []
    for worker, object, label in iter_assigns(fname):
        i += 1
        w.append((worker, object, label))
        if i == APACK_SIZE:
            yield w
            w = []
            i = 0
    if w:
        yield w


def post_assigns(tc, fname):
    assigns_packages = load_assigns(fname)
    for i, package in enumerate(assigns_packages):
        print i * APACK_SIZE,
        sys.stdout.flush()
        wc(tc, tc.post_assigned_labels(package))


def post_golds(tc, fname):
    with open(fname, 'r') as F:
        golds = [f.strip().split(" ") for f in F]
        golds = [(x[0], " ".join(x[1:])) for x in golds]
        wc(tc, tc.post_gold_data(golds))

def compute(tc):
    return wc(tc, tc.post_compute())


def get_res(tc, resp):
    return wc(tc, resp)['result']


def get_objects_results(tc, cost_alg):
    return [(d['objectName'], d['categoryName']) for d in
            get_res(tc, tc.get_objects_prediction(cost_alg))]


def get_workers_results(tc, cost_alg):
    return [(d['workerName'], d['value']) for d in
            get_res(tc, tc.get_estimated_workers_quality(cost_alg))]


def sort_results(results):
    return sorted(results, key=lambda x: int(x[0]))


def save(results, kind, cost_alg):
    fname = "%s_%d_%s_%s.tsv" % (ALGORITHM, ITERATIONS, kind, cost_alg)
    with open(fname, 'w') as F:
        for r in results:
            F.write("%s\t%s\n" % r)


def process_results(tc):
    for cost_alg in ["MinCost", "MaxLikelihood"]:
        short_save = lambda results, kind: save(sort_results(results), kind, cost_alg)
        obj_results = get_objects_results(tc, cost_alg)
        short_save(obj_results, "objects")
        work_results = get_workers_results(tc, cost_alg)
        short_save(work_results, "workers")


def main(args):
    assigns_fname = args[0]
    tc = create(list(get_categories(assigns_fname)))
    print tc.jid
    post_assigns(tc, assigns_fname)
    if len(args) > 1:
        golds_fname = args[1]
        post_golds(tc, golds_fname)
    pprint.pprint(compute(tc))
    process_results(tc)


if __name__ == '__main__':
    main(sys.argv[1:])

