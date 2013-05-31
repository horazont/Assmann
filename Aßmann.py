#!/usr/bin/python3

import re
import sys
import pickle
import functools
import operator

import argparse

def positive_int(s):
    i = int(s)
    if i <= 0:
        raise ValueError("Out of bounds: {}".format(i))
    return i

SOURCES = ('plain', 'gajim', 'maildir')
def source_type(str_):
    if str_ in SOURCES:
        return str_
    else:
        raise argparse.ArgumentTypeError("source type must be one of {}"
                                         .format(SOURCES))

class LearnWords:
    pattern = """(\w+|\s+|,|\.|\?|!|"|'|\[|\]|\(|\)|\n)"""

    def __init__(self, args):
        if args.source_type == 'plain':
            self.source = self.plain_source
        elif args.source_type == 'gajim':
            self.source = self.gajim_source
            self._jidid = args.jidid
            self._contact_name = args.contact_name
        elif args.source_type == 'maildir':
            self.source = self.maildir_source
            self._folder = args.folder

        self._order = args.order
        self._infile = args.infile
        self._encoding = args.encoding
        self._chainfile = args.chainfile

        if args.fold_whitespace:
            self._filter = self.filter_fold_whitespace
        else:
            self._filter = self.filter_pass

        if args.fold_case:
            old_filter = self._filter
            self._filter = lambda x: str.lower(old_filter(x))

    @staticmethod
    def filter_fold_whitespace(x):
        x = x.replace("\t", " ").replace("\n", " ").strip()
        return x if x else " "

    @staticmethod
    def filter_pass(x):
        return x

    def filter_text(self, text):
        """Filter an input text using self.pattern and the global folding
        settings. Used by both plain_source() and gajim_source().
        """
        yield from map(self._filter,
                       (m.group(0) for m in re.finditer(self.pattern, text)))


    def plain_source(self):
        infile = open(self._infile, "r", encoding=self._encoding)
        with infile as f:
            yield from self.filter_text(f.read())

    def gajim_source(self):
        import sqlite3

        with sqlite3.connect(self._infile) as conn:
            c = conn.cursor()

            fragment = ''
            data = [str(self._jidid)]
            if self._contact_name:
                fragment = 'and contact_name = ?'
                data.append(self._contact_name)

            query = ( "select message from logs where jid_id = ? {} and "
                      "kind = 2" ).format(fragment)

            c.execute(query, data)

            yield from self.filter_text('\n'.join((r[0] for r in c)))

    @classmethod
    def get_plaintext_parts(cls, msg):
        """Recursively retrieve plain text message parts from a Message
           instance.
        """

        if msg.is_multipart():
            for payload in msg.get_payload():
                yield from cls.get_plaintext_parts(payload)
        else:
            if msg.get_content_type() == "text/plain":
                yield msg


    def __call__(self):
        print("learning ... ", end="")
        sys.stdout.flush()
        chain = MarkovChain.MarkovChain(self._order)
        chain.learn(self.source())
        print("done.")

        with open(self._chainfile, "wb") as f:
            pickle.dump(chain, f)

class Produce:
    def __init__(self, args):
        with open(args.chainfile, "rb") as f:
            self._chain = pickle.load(f)
        self._units = args.units
        if not args.fixed_state:
            self._chain.set_random_state()

    def __call__(self):
        iterable = self._chain.emit()
        try:
            for i in range(self._units):
                print(next(iterable), end="")
        except StopIteration:
            pass
        print()

class Merge:
    @staticmethod
    def open_chain(filename):
        return open(filename, "rb"), filename

    @staticmethod
    def load_chain(f):
        try:
            return pickle.load(f)
        finally:
            f.close()

    def __init__(self, args):
        # first open all chains to make sure they exist and are
        # readable
        self._chains = list(map(self.open_chain, args.chains))
        self._outfile = args.outfile

    def __call__(self):
        with open(self._outfile, "wb") as outf:
            chain_iter = iter(self._chains)
            f, filename = next(chain_iter)
            print("loading first chain ({})... ".format(filename), end="")
            sys.stdout.flush()
            chain = self.load_chain(f)
            print("ok.")
            for f, filename in chain_iter:
                print("loading {}... ".format(filename), end="")
                sys.stdout.flush()
                next_chain = self.load_chain(f)
                print("ok.")

                print("merging {}... ".format(filename), end="")
                sys.stdout.flush()
                chain += next_chain
                print("ok.")
                del next_chain

            print("writing output ({})... ".format(self._outfile), end="")
            sys.stdout.flush()
            pickle.dump(chain, outf)
            outf.flush()
            print("ok.")

if __name__ == "__main__":
    import MarkovChain

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()
    learn_parser = subparsers.add_parser(
        "learn-words",
        help="Learn the markov chain from a source file"
    )
    learn_parser.set_defaults(cls=LearnWords)
    learn_parser.add_argument(
        "--source-type",
        type=source_type,
        default='plain',
        metavar="TYPE",
        help="""source type to learn from. Must be one of {}. Defaults to
        'plain'.""".format(SOURCES)
    )
    learn_parser.add_argument(
        "infile",
        metavar="INFILE",
        help="""File to learn from. Must be plaintext or a gajim DB
        (when called with --source gajim)."""
    )
    learn_parser.add_argument(
        "--jidid",
        metavar="JIDID",
        type=positive_int,
        default=None,
        help="Only learn messages from this jid_id from gajim logs."
    )
    learn_parser.add_argument(
        "--contact-name",
        metavar="NAME",
        default=None,
        help="Only learn messages from this contact_name from gajim logs."
    )
    learn_parser.add_argument(
        "--folder",
        metavar="FOLDER",
        default=None,
        help="Specify subfolder when learning from maildir."
    )
    learn_parser.add_argument(
        "order",
        type=positive_int,
        help="Order of the markov chain. Must be a positive integer."
    )
    learn_parser.add_argument(
        "chainfile",
        metavar="OUTFILE",
        help="File to store the learned data to."
    )
    learn_parser.add_argument(
        "--encoding",
        metavar="CODING",
        default="utf-8",
        help="Encoding used for the input file, defaults to utf-8"
    )
    learn_parser.add_argument(
        "--fold-whitespace",
        action="store_true",
        help="Map all whitespace to plain spaces"
    )
    learn_parser.add_argument(
        "--fold-case",
        action="store_true",
        help="Map all characters to lower case"
    )

    produce_parser = subparsers.add_parser(
        "produce",
        help="Load a saved markov chain and produce data from it"
    )
    produce_parser.set_defaults(cls=Produce)
    produce_parser.add_argument(
        "chainfile",
        help="Chain file to load"
    )
    produce_parser.add_argument(
        "units",
        metavar="NUMBER",
        type=positive_int,
        help="Produce NUMBER units of output"
    )
    produce_parser.add_argument(
        "--fixed-state",
        default=False,
        action="store_true",
        help="""Initializes the chain to the fixed zeroth state, giving
        a fixed start for the text."""
    )

    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge two or more chains together"
    )
    merge_parser.set_defaults(cls=Merge)
    merge_parser.add_argument(
        "outfile",
        help="File to write the merged chains to"
    )
    merge_parser.add_argument(
        "chains",
        nargs="+",
        help="Source files to load and merge together"
    )

    args = parser.parse_args()

    try:
        cls = args.cls
    except AttributeError:
        parser.print_help()
        sys.exit(0)

    instance = args.cls(args)
    instance()
