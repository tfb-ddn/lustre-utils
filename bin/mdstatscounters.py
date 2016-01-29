#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
mdtstatscounters.py is a simple utility script written in Python that parses
mdt_stats and displays delta between two measures.
"""

from __future__ import print_function

import sys
import time
import signal
import io
import csv
from optparse import OptionParser
from collections import OrderedDict

__author__ = "Thomas Favre-Bulle"
__version__ = "1.1"
__license__ = "GPL"

# Command usage, full help is automatically generated
USAGE = "%prog [OPTIONS] -t|--target=<target>"

# Basic return codes
EXIT_OK = 0
EXIT_ERROR = 1

def signal_handler(sign, frame):
    """
    Handles SIGINT signal, clean exit
    @param sign signal to handle
    @param frame
    """
    print("Exiting")
    sys.exit(EXIT_OK)

def parse_cmdline():
    """
    parse_cmdline parses supplied options and returns them
    @return parsed options and args
    """
    parser = OptionParser(usage=USAGE)
    parser.add_option('-t', '--target', dest='target', action='store',
                      type='string', help='MDT name to monitor, \
                      <fsname>-MDT<index> default: %default',
                      default='scratch-MDT0000')
    parser.add_option('--no-header', dest='noheader', action='store_true',
                      help='Don\'t print header, default: %default',
                      default=False)
    parser.add_option('--table', dest='table', action='store_true',
                      help='Print values as a table, default: %default',
                      default=False)
    parser.add_option('--csv', dest='csv', action='store_true',
                      help='Print values in CSV format, default: %default',
                      default=False)
    parser.add_option('-i', '--intreval', dest='interval', action='store',
                      type='int', help='Statistics print interval in seconds, \
                      default: %default', default=1)
    parser.add_option('-c', '--count', dest='count', action='store', type='int',
                      help='Number of iteration', default=0)

    (options, args) = parser.parse_args()

    if not options.interval > 0:
        parser.error("interval must be greater than 0 !")

    parser.destroy()

    return options, args

def read_stats(target):
    """
    Read Lustre MDT stats from /proc
    Works as of Lustre 2.5+
    @param target a string containing the name of the MDT target
    @return a dictionnary object containing MDT stats
    """
    with open('/proc/fs/lustre/mdt/' + target + '/md_stats', 'r') as f:
        data = f.readlines()

    values = list()
    for entry in data:
        entry = entry.split()
        del entry[2:]
        values.append(entry)

    return dict(e for e in values)

def print_header(data, table=False, printcsv=False):
    """
    Print statistics header
    @param data a dictionnary which must contain all keys to be displayed
    @param table switch to display data as table
    @param printcsv switch to output data as CSV parseable
    """
    if printcsv:
        output = io.BytesIO()
        writer = csv.writer(output)
        writer.writerow(data.keys())
        print(output.getvalue().rstrip())
    else:
        if table:
            print("{0:<18} {1:>8} {2:>8} {3:>8} {4:>8} {5:>8} {6:>8} \
            {7:>8} {8:>8} {9:>8} {10:>8} {11:>8} {12:>8} {13:>8} \
            {14:>8} {15:>14}".format(*data.keys()))
        else:
            print("{0:<18} {1:<10}".format('Stat', 'Value'))

def print_stats(data, table=False, printcsv=False):
    """
    Print MDT statistics
    @param data a dictionnary of MDT stats
    @param table switch to display data as table
    @param printcsv switch to output data as CSV parseable
    """
    if printcsv:
        output = io.BytesIO()
        writer = csv.DictWriter(output, fieldnames=data.keys())
        writer.writerow(data)
        print(output.getvalue().rstrip())
    else:
        if table:
            print("{0:<18} {1:>8} {2:>8} {3:>8} {4:>8} {5:>8} {6:>8} \
            {7:>8} {8:>8} {9:>8} {10:>8} {11:>8} {12:>8} {13:>8} \
            {14:>8} {15:>14}".format(*data.values()))
        else:
            for key in data:
                print("{0:<18} {1:<10}".format(key, data[key]))

def main():
    """
    Main program execution loop
    """
    # Add SIGINT signal handling
    signal.signal(signal.SIGINT, signal_handler)

    rc = EXIT_ERROR

    # Parse command line
    (options, args) = parse_cmdline()

    sleep_interval = options.interval
    max_loop = options.count

    # Some values may not be present in mdt_stats, we have to initialise that
    # dictionnary entirely to avoid missing values
    previous_stats = OrderedDict()
    previous_stats['snapshot_time'] = 0
    previous_stats['open'] = 0
    previous_stats['close'] = 0
    previous_stats['mknod'] = 0
    previous_stats['link'] = 0
    previous_stats['unlink'] = 0
    previous_stats['mkdir'] = 0
    previous_stats['rmdir'] = 0
    previous_stats['rename'] = 0
    previous_stats['getattr'] = 0
    previous_stats['setattr'] = 0
    previous_stats['getxattr'] = 0
    previous_stats['setxattr'] = 0
    previous_stats['statfs'] = 0
    previous_stats['sync'] = 0
    previous_stats['samedir_rename'] = 0

    deltas = previous_stats.copy()

    if not options.noheader:
        print_header(deltas, table=options.table, printcsv=options.csv)

    current_loop = 1
    while 1:
        current_stats = read_stats(options.target)

        for stat in current_stats:
            if stat == 'snapshot_time':
                previous_stats[stat] = current_stats[stat]
                deltas[stat] = current_stats[stat]
                continue
            delta = int(current_stats[stat]) - previous_stats[stat]
            deltas[stat] = delta
            previous_stats[stat] = int(current_stats[stat])

        if max_loop > 0:
            if current_loop > max_loop:
                break
            else:
                current_loop += 1
        print_stats(deltas, table=options.table, printcsv=options.csv)
        time.sleep(sleep_interval)

    return rc

if __name__ == '__main__':
    sys.exit(main())
