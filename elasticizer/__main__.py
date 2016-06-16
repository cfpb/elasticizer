import sys
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='elasticizer',
                                     description='from DB to Elasticsearch')
    subs = parser.add_subparsers(help='available commands')
    
    commands = {
#                'worker': elasticizer.Worker,
                }

    # for name in sorted(commands):
    #     cls = commands[name]
    #     sub1 = subs.add_parser(name, help=cls.description)
    #     cls.addParserArguments(sub1)
    
    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_help()
    elif len(sys.argv) >= 2 and sys.argv[1].lower() in commands:
        cls = commands[sys.argv[1].lower()]
        cls.run(**vars(args))