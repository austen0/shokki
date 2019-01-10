"""shokki distributes repeated function calls across system resources.

Usage:

    import shokki

    def dummyfn(x):
        return 'foo ' + x

    s = shokki.Shuttle()
    print s.threadit(dummyfn, [bar, qux])  # ['foo bar', 'foo qux']
"""


__author__ = 'austen0'
__version__ = '0.0.1'


import threading
import multiprocessing

# Dev only
from time import sleep
from random import randint


class Shuttle:

    def __init__(self, thread_limit=None):
        # Default thread limit to 85% of local CPU cores (rounded down).
        partial_load = multiprocessing.cpu_count() * 85 / 100
        self.thread_limit = partial_load if not thread_limit else thread_limit

        # Threads will store return values and states in common dictionaries.
        self.returns = dict()
        self.thread_state = dict()
        self.exit_flag = False

    def threadit(self, worker, args, arg_type='auto'):
        # Validate arg type and input structure.
        def validate_args(args, arg_type):
            # Note auto-resolve and attempt to determine type.
            arg_type_ = arg_type
            if arg_type == 'auto':
                if (
                        isinstance(args, list)
                        and isinstance(args[0], list)
                        and isinstance(args[1], dict)):
                    arg_type = 'both'
                elif isinstance(args, list):
                    arg_type = 'args'
                elif isinstance(args, dict):
                    arg_type = 'kwargs'

            if arg_type == 'args' and isinstance(args, list):
                return 'args'
            elif arg_type == 'kwargs' and isinstance(args, dict):
                return 'kwargs'
            elif (
                    arg_type == 'both' and
                    isinstance(args, list)
                    and isinstance(args[0], list)
                    and isinstance(args[1], dict)):
                return 'both'
            else:
                if arg_type_ != arg_type:
                    arg_type = 'auto_resolve:%s' % arg_type
                self.exit_flag = True
                raise ValueError(
                        "args not passed in right format '%s'" % arg_type)

        # Nest the function to be threaded under a sub-function to allow
        # storing of return values in shared class variables.
        def worker_return_handler(
                self, worker, thread_num, args, arg_type=arg_type):
            arg_type = validate_args(args, arg_type)
            if arg_type == 'args':
                self.returns[thread_num] = worker(*args)
            elif arg_type == 'kwargs':
                self.returns[thread_num] = worker(**args)
            elif arg_type == 'both':
                self.returns[thread_num] = worker(*args[0], **args[1])
            self.thread_state[thread_num] = 0

        # Spawn a thread for each set of args being passed to function.
        i = 0
        for a in args:
            self.thread_state[i] = 1
            t = threading.Thread(
                target=worker_return_handler, args=(self, worker, i, a))
            t.daemon
            t.start()
            i += 1

        # Wait until all threads are finished.
        while sum(self.thread_state.values()) > 0 and not self.exit_flag:
            pass

        # Return the thread return values back as an array matching the order in
        # which the arguments were originally passed to threadit().
        return_vals = []
        for i in range(len(self.returns)):
            return_vals.append(self.returns[i])
        return return_vals


def dummyfn(a, b=None):
    sleep(randint(1, 5))
    return 'foo %s %s' % (a, b)

def main():
    s = Shuttle()
    print s.threadit(
        dummyfn,
        [
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
            [['bar'], {'b': 'qux'}],
        ],
        arg_type='auto'
    )


if __name__ == '__main__':
    main()
