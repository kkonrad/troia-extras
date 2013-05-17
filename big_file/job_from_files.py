import sys

from client.gal import TroiaClient

TROIA_ADDRESS = 'http://localhost:8080/service'

ALGORITHM = "BDS"

ITERATIONS = 10
APACK_SIZE = 5000


def create():
    categories = ["broken", "invalid", "matched", "new", "reconciled", "skip"]
    tc = TroiaClient(TROIA_ADDRESS)
    tc.create(categories, algorithm=ALGORITHM, iterations=ITERATIONS)
    return tc


def wc(tc, resp):
    resp = tc.await_completion(resp)
    if resp['status'] != 'OK':
        import pprint
        pprint.pprint(resp)
        assert False


def load_assigns():
    with open('/home/konrad/troia/answers.out', 'r') as F:
        i = 0
        w = []
        for l in F:
            l = l.strip()
            i += 1
            object, worker, label = l.split(' ')
            w.append((worker, object, label))
            if i == APACK_SIZE:
                yield w
                w = []
                i = 0
        if w:
            yield w


def post_assigns(tc):
    assigns_packages = load_assigns()
    for i, package in enumerate(assigns_packages):
        print i * APACK_SIZE,
        wc(tc, tc.post_assigned_labels(package))


def compute(tc):
    wc(tc, tc.post_compute())


def main(args):
    tc = create()
    print tc.jid
    post_assigns(tc)
    compute(tc)


if __name__ == '__main__':
    main(sys.argv[1:])

