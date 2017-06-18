import logging
from random import uniform, randrange
from time import sleep
from argparse import ArgumentParser

import redis
import dill

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


r = redis.Redis(
    host='192.168.0.17',
    port=6379
)

def do_something(arg1, arg2):
    """ Dummy function that just waits a random amount of time """
    logger.info("Performing task with arg1=%s and arg2=%s", arg1, arg2)
    sleep(uniform(0.0, 1))
    return uniform(0.0, 1)


def create_jobs():
    # Generate N tasks
    NUM_TASKS = 5
    logger.info("Generating %i tasks", NUM_TASKS)
    for i in range(NUM_TASKS):
        # Generate two random arguments                                                         
        a1 = randrange(0, 100)
        a2 = randrange(0, 100)
        # Serialize the task and its arguments                                                 
        data = dill.dumps((do_something, [a1, a2]))
        # Store it in the message broker                                                    
        r.lpush('todo', data)

def worker():
    while True:
        # Wait until there's an element in the 'tasks' queue
        if r.llen('todo') == 0:
            logger.info("No more tasks")
            return
        key, data = r.brpop('todo')
        try:
            # Deserialize the task
            d_fun, d_args = dill.loads(data)

            # Run the task
            result = d_fun(*d_args)
            r.lpush('result',result)
        except:
            r.lpush('todo',data)
            return

def print_results():
    print("\nRESULTS")
    print(r.lrange('result',0,r.llen('result')))
    print("\n{} tasks remaining".format(r.llen('tasks')))

if __name__ == "__main__":
    parser = ArgumentParser(description='A simple task queue')
    parser.add_argument('--create',
        action='store_true',
        help='create jobs' )
    parser.add_argument('--worker',
        action='store_true',
        help='run as a worker' )
    parser.add_argument('--results',
        action='store_true',
        help='print results' )

    args = parser.parse_args()

    if args.create:
        create_jobs()
    elif args.worker:
        worker()
    elif args.results:
        print_results()
    else:
        parser.print_help()
