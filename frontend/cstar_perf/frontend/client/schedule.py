import argparse 
from api_client import APIClient

class Scheduler(APIClient):
    def schedule(self, job):
        """Schedule a job. job can either be a path to a file, or a dictionary"""
        if isinstance(job, basestring):
            with open(job) as f:
                job = f.read()
        else:
            job = json.dumps(job)

        print self.post("/tests/schedule", data=job)
        

def main():
    parser = argparse.ArgumentParser(description='cstar_perf job scheduler', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-s', '--server', default='localhost:8000',
                        help='Server endpoint', dest='server')
    parser.add_argument(
        'job', help='The JSON job description file', nargs='+')
    args = parser.parse_args()

    scheduler = Scheduler(args.server)

    for job in args.job:
        scheduler.schedule(job)


if __name__ == "__main__":
    main()
