"""shokki distributes repeated function calls across system resources.

Usage:

    import shokki

    def dummyfn(x):
        return 'foo ' + x

    s = shokki.Shokki()
    print s.threadit(dummyfn, [bar, qux])  # ['foo bar', 'foo qux']
"""


__author__ = 'austen0'
__version__ = '0.0.1'


import threading
import multiprocessing
from time import sleep


class Shokki:

    def __init__(self, thread_limit=None):
        # Default thread limit to 85% of local CPU cores (rounded down).
        partial_load = multiprocessing.cpu_count() * 85 / 100
        if thread_limit == None:
            self.thread_limit = partial_load
        else:
            self.thread_limit = thread_limit

        # Threads will store return values and states in common dictionaries.
        self.returns = dict()
        self.thread_state = dict()
        self.exit_flag = False

    def threadit(self, worker, args, arg_type=None):
        # Validate arg type and input structure.
        def validate_args(args, arg_type):
            input_type = {'args': False, 'kwargs': False, 'combo': False}
            if isinstance(args, list):
                input_type['args'] = True
            if isinstance(args, dict):
                input_type['kwargs'] = True
            if isinstance(args[0], list) and isinstance(args[1], dict):
                input_type['combo'] = True

            if (arg_type == 'combo' or not arg_type) and input_type['combo']:
                return 'combo'
            elif (arg_type == 'args' or not arg_type) and input_type['args']:
                return 'args'
            elif (arg_type == 'kwargs' or not arg_type) and input_type['kwargs']:
                return 'kwargs'
            else:
                if not arg_type:
                    arg_type = 'auto_resolve'
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
            elif arg_type == 'combo':
                self.returns[thread_num] = worker(*args[0], **args[1])
            self.thread_state[thread_num] = 0

        # Spawn a thread for each set of args being passed to function.
        i = 0
        for a in args:
            # Pause if already at thread limit.
            while (
                    sum(self.thread_state.values()) > self.thread_limit
                    and self.thread_limit != 0):
                pass
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
