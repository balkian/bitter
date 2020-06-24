from __future__ import print_function

import click
import json
import os
import logging
import time
import sqlalchemy.types
import threading
import sqlite3
from tqdm import tqdm

from sqlalchemy import exists

from bitter import utils, models, crawlers
from bitter import config as bconf
from bitter.models import make_session, User, ExtractorEntry, Following

import sys
if sys.version_info <= (3, 0):
    from contextlib2 import ExitStack
else:
    from contextlib import ExitStack



logger = logging.getLogger(__name__)

def serialize(function):
    '''Common options to serialize output to CSV or other formats'''

    @click.option('--fields', help='Provide a list of comma-separated fields to print.', default='', type=str)
    @click.option('--ignore_missing', help='Do not show warnings for missing fields.', is_flag=True)
    @click.option('--header', help='Header that will be printed at the beginning of the file', default=None)
    @click.option('--csv', help='Print each object as a csv row.', is_flag=True)
    @click.option('--jsonlines', '--json', help='Print each object as JSON in a new line.', is_flag=True)
    @click.option('--indented', help='Print each object as an indented JSON object', is_flag=True)
    @click.option('--outdelimiter', help='Delimiter for some output formats, such as CSV. It defaults to \t', default='\t')
    @click.option('--outfile', help='Output file. It defaults to STDOUT', default=sys.stdout)
    def decorated(fields, ignore_missing, header, csv, jsonlines, indented, outfile, outdelimiter, **kwargs):
        it = function(**kwargs)
        outformat = 'json'
        if csv:
            outformat = 'csv'
        elif jsonlines:
            outformat = 'jsonlines'
        elif indented:
            outformat = 'indented'

        return utils.serialized(it, outfile, outformat=outformat, fields=fields.split(','), ignore_missing=ignore_missing, header=header, delimiter=outdelimiter)

    return decorated


@click.group()
@click.option("--verbose", is_flag=True)
@click.option("--logging_level", required=False, default='WARN')
@click.option('--config', show_default=True, default=bconf.CONFIG_FILE)
@click.option('--credentials', show_default=True, help="DEPRECATED: If specified, these credentials will be copied to the configuratation file.", default=bconf.CREDENTIALS)
@click.pass_context
def main(ctx, verbose, logging_level, config, credentials):
    logging.basicConfig(level=getattr(logging, logging_level))
    ctx.obj = {}
    ctx.obj['VERBOSE'] = verbose
    bconf.CONFIG_FILE = config
    bconf.CREDENTIALS = credentials
    if os.path.exists(utils.get_config_path(credentials)):
      utils.copy_credentials_to_config(credentials, config)


@main.group(invoke_without_command=True)
@click.pass_context
def credentials(ctx):
    if ctx.invoked_subcommand is not None:
        return
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    for worker in wq.queue:
        print('#'*20)
        try:
            resp = worker.client.application.rate_limit_status()
            print(worker.name)
        except Exception as ex:
            print('{}: AUTHENTICATION ERROR: {}'.format(worker.name, ex) )


@credentials.command('limits')
@click.option('--no_aggregate', is_flag=True, default=False,
              help=('Print limits from all workers. By default, limits are '
                    'aggregated (summed).'))
@click.option('--no_diff', is_flag=True, default=False,
              help=('Print all limits. By default, only limits that '
                    'have been consumed will be shown.'))
@click.argument('url', required=False)
@click.pass_context
def get_limits(ctx, no_aggregate, no_diff, url):
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    limits = {}
    if url:
        print('URL is: {}'.format(url))
    for worker in wq.queue:
        resp = worker.client.application.rate_limit_status()
        for urlimits in resp['resources'].values():
            for url, value in urlimits.items():
                if url not in limits:
                    limits[url] = {}
                glob = limits[url].get('global', {})
                limits[url][worker.name] = value
                for k in ['limit', 'remaining']:
                    if k not in glob:
                        glob[k] = 0
                    glob[k] += value[k]
                limits[url]['global'] = glob
    for url, lims in limits.items():
        worker_list = lims.keys() if no_aggregate else ['global', ] 

        url_printed = False

        for worker in worker_list:
            vals = lims[worker]
            consumed = vals['limit'] - vals['remaining'] 
            if no_diff or consumed:
                if not url_printed:
                    print(url)
                    url_printed = True
                print('\t', worker, ':')
                print('\t\t', vals)


@credentials.command('add')
@click.option('--consumer_key', default=None)
@click.option('--consumer_secret', default=None)
@click.option('--token_key', default=None)
@click.option('--token_secret', default=None)
@click.argument('user_name')
def add(user_name, consumer_key, consumer_secret, token_key, token_secret):
    if not consumer_key:
        consumer_key = click.prompt('Please, enter your YOUR CONSUMER KEY')
    if not consumer_secret:
        consumer_secret = click.prompt('Please, enter your CONSUMER SECRET')
    if not token_key:
        token_key = click.prompt('Please, enter your ACCESS TOKEN')
    if not token_secret:
        token_secret = click.prompt('Please, enter your ACCESS TOKEN SECRET')
    utils.add_credentials(conffile=bconf.CONFIG_FILE, user=user_name, consumer_key=consumer_key, consumer_secret=consumer_secret,
                          token_key=token_key, token_secret=token_secret)
    click.echo('Credentials added for {}'.format(user_name))


@main.group()
@click.pass_context
def tweet(ctx):
    pass

@tweet.command('get')
@click.option('-d', '--dry_run', is_flag=True, default=False)
@click.option('-f', '--folder', default="tweets")
@click.option('-u', '--update', help="Update the file even if the tweet exists", is_flag=True, default=False)
@click.argument('tweetid')
@serialize
def get_tweet(tweetid, dry_run, folder, update):
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    yield from utils.download_tweet(wq, tweetid, not dry_run, folder, update)

@tweet.command('get_all', help='''Download tweets from a list of tweets in a CSV file.
The result is stored as individual json files in your folder of choice.''')
@click.argument('tweetsfile')
@click.option('-f', '--folder', default="tweets")
@click.option('-u', '--update', is_flag=True, default=False, help='Download tweet even if it is already present. WARNING: it will overwrite existing files!')
@click.option('-r', '--retry', is_flag=True, default=False, help='Retry failed downloads')
@click.option('-d', '--delimiter', default=",")
@click.option('-n', '--nocache', is_flag=True, default=False, help='Do not cache results')
@click.option('--skip', help='Discard the first DISCARD lines (use them as a header)', default=0)
@click.option('--commentchar', help='Lines starting with this character will be ignored', default=None)
@click.option('-q', '--quotechar', default='"')
@click.option('-c', '--column', type=int, default=0)
@serialize
@click.pass_context
def get_tweets(ctx, tweetsfile, folder, update, retry, delimiter, nocache, skip, quotechar, commentchar, column):
    if update and not click.confirm('This may overwrite existing tweets. Continue?'):
        click.echo('Cancelling')
        return
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)

    status = tqdm('Queried')
    failed = 0
    for tid, obj in utils.download_tweets_file(wq, tweetsfile, folder, delimiter=delimiter, cache=not nocache,
                                               skip=skip, quotechar=quotechar, commentchar=commentchar,
                                               column=column, update=update, retry_failed=retry):
        status.update(1)
        if not obj:
            failed += 1
            status.set_description('Failed: %s. Queried' % failed, refresh=True)
            continue
        yield obj


@tweet.command('search')
@click.argument('query')
@serialize
@click.pass_context
def search(ctx, query):
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    yield from utils.search_tweet(wq, query)

@tweet.command('timeline')
@click.argument('user')
@click.pass_context
def timeline(ctx, user):
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    t = utils.user_timeline(wq, user)
    print(json.dumps(t, indent=2))

@main.group()
@click.pass_context
def users(ctx):
    pass

@users.command('list')
@click.option('--db', required=True, help='Database of users.')
@click.pass_context
def list_users(ctx, db):
    dburl = 'sqlite:///{}'.format(db)
    session = make_session(dburl)
    for i in session.query(User):
        print(i.screen_name)
        for j in i.__dict__:
            print('\t{}: {}'.format(j, getattr(i,j)))

@users.command('get')
@click.argument('user')
@click.option('-d', '--dry_run', is_flag=True, default=False)
@click.option('-f', '--folder', default="users")
@click.option('-u', '--update', help="Update the file even if the user exists", is_flag=True, default=False)
@serialize
def get_user(user, dry_run, folder, update):
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    yield from utils.download_user(wq, user, not dry_run, folder, update)

@users.command('get_all', help='''Download users from a list of user ids/screen names in a CSV file.
               The result is stored as individual json files in your folder of choice.''')
@click.argument('usersfile')
@click.option('-f', '--folder', default="users")
@click.option('-u', '--update', is_flag=True, default=False, help='Download user even if it is already present. WARNING: it will overwrite existing files!')
@click.option('-r', '--retry', is_flag=True, default=False, help='Retry failed downloads')
@click.option('-n', '--nocache', is_flag=True, default=False, help='Do not cache results')
@click.option('-d', '--delimiter', default=",")
@click.option('--skip', help='Discard the first SKIP lines (e.g., use them as a header)',
              is_flag=True, default=False)
@click.option('-q', '--quotechar', default='"')
@click.option('--commentchar', help='Lines starting with this character will be ignored', default=None)
@click.option('-c', '--column', type=int, default=0)
@serialize
@click.pass_context
def get_users(ctx, usersfile, folder, update, retry, nocache, delimiter, skip, quotechar, commentchar, column):
    if update and not click.confirm('This may overwrite existing users. Continue?'):
        click.echo('Cancelling')
        return
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    for i in utils.download_users_file(wq, usersfile, folder, delimiter=delimiter,
                                       update=update, retry_failed=retry,
                                       skip=skip, quotechar=quotechar,
                                       cache=not nocache,
                                       commentchar=commentchar,
                                       column=column):
        yield i

@users.command('crawl')
@click.option('--db', required=True, help='Database to save all users.')
@click.option('--skip', required=False, default=0, help='Skip N lines from the file.')
@click.option('--until', required=False, type=str, default=0, help='Skip all lines until ID.')
@click.option('--threads', required=False, type=str, default=20, help='Number of crawling threads.')
@click.argument('usersfile')
@click.pass_context
def crawl_users(ctx, usersfile, skip, until, threads, db):
    global dburl, ids_queue, skipped, enqueued, collected, lastid, db_lock

    if '://' not in db:
        dburl = 'sqlite:///{}'.format(db)
        db_lock = threading.Lock()
    else:
        dburl = db
        def db_lock():
            return ExitStack()


    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    logger.info('Starting Network crawler with {} threads and {} credentials.'.format(threads,
                                                                                      len(wq.queue)))

    ids_queue = queue.Queue(1000)
    skipped = skip
    enqueued = 0
    collected = 0
    statslock = threading.Lock()
    lastid = -1

    def fill_queue():
        global enqueued, skipped
        with open(usersfile, 'r') as f:
            sqlite = sqlite3.connect(db)
            engine = sqlalchemy.create_engine(dburl)
            def user_filter(x):
                global skipped, dburl
                # keep = data['users'].find_one(id=x) is None
                #keep = not session.query(exists().where(User.id == x)).scalar()
                # keep = session.engine.execute
                keep = not list(engine.execute('SELECT 1 from users where id=\'%s\'' % x))

                if not keep:
                    skipped += 1
                return keep
            for i in range(skip):
                next(f)
            ilist = map(lambda x: x.strip(), f)
            logger.info('Skipping until {}'.format(until))
            if not skip and until:
                for uid in ilist:
                    if uid == until:
                        break
                    else:
                        skipped += 1
            ilist = filter(user_filter, ilist)
            for uid in ilist:
                ids_queue.put(uid)
                enqueued += 1
        for i in range(threads):
            ids_queue.put(None)

    def consume_queue():
        global dburl, collected, ids_queue, lastid
        local_collected = 0
        logging.debug('Consuming!')
        session = make_session(dburl)
        q_iter = iter(ids_queue.get, None)
        for user in utils.get_users(wq, q_iter):
            dbuser = User(**user)
            session.add(dbuser)
            local_collected += 1
            with statslock:
                collected += 1
                lastid = user['id']
            if local_collected % 100 == 0:
                with db_lock:
                    session.commit()
        session.commit()
        logger.debug('Done consuming')

    filler = threading.Thread(target=fill_queue)
    filler.start()
    consumers = [threading.Thread(target=consume_queue) for i in range(threads)]
    logging.debug('Starting consumers')
    for c in consumers:
        c.start()
    logging.debug('Joining filler')
    counter = 0
    speed = 0
    lastcollected = collected
    while True:
        filler.join(1)
        logger.info('########\n'
                    '   Collected: {}\n'
                    '   Speed: ~ {} profiles/s\n'
                    '   Skipped: {}\n'
                    '   Enqueued: {}\n'
                    '   Queue size: {}\n'
                    '   Last ID: {}'.format(collected, speed, skipped, enqueued, ids_queue.qsize(), lastid))
        if not filler.isAlive():
            if all(not i.isAlive() for i in consumers):
                break
            else:
                time.sleep(1)
        counter += 1
        if counter % 10 == 0:
            speed = (collected-lastcollected)/10
            with statslock:
                lastcollected = collected

    logger.info('Done!')

@main.group('extractor')
@click.pass_context
@click.option('--db', required=True, help='Database of users.')
def extractor(ctx, db):
    if '://' not in db:
        db = 'sqlite:///{}'.format(db)
    ctx.obj['DBURI'] = db
    ctx.obj['SESSION'] = make_session(db)


@extractor.command('status')
@click.option('--with_followers', is_flag=True, default=False)
@click.option('--with_not_pending', is_flag=True, default=False)
@click.pass_context
def status_extractor(ctx, with_followers, with_not_pending):
    session = ctx.obj['SESSION']
    entries = session.query(ExtractorEntry)
    if not with_not_pending:
        entries = entries.filter(ExtractorEntry.pending==True)
    for i in entries:
        print(i.id)
        for j in i.__dict__:
            print('\t{}: {}'.format(j, getattr(i,j)))
    followers = session.query(Following)
    print('Followers count: {}'.format(followers.count()))
    if(with_followers):
        for i in followers:
            print(i.id)
            for j in i.__dict__:
                print('\t{}: {}'.format(j, getattr(i,j)))

@extractor.command('network')
@click.option('--as_json', is_flag=True, default=False)
@click.pass_context
def network_extractor(ctx, as_json):
    session = ctx.obj['SESSION']
    followers = session.query(Following)
    follower_map = []
    for i in followers:
        if not as_json:
            print('{} -> {}'.format(i.follower, i.isfollowed))
        else:
            follower_map.append({'source_id': i.follower,
                                 'target_id': i.isfollowed,
                                 'following': True})
    if as_json:
        import json
        print(json.dumps(follower_map, indent=4))


@extractor.command('users')
@click.pass_context
def users_extractor(ctx):
    session = ctx.obj['SESSION']
    users = session.query(User)
    import json
    for i in users:
        # print(json.dumps(i.as_dict(), indent=4))
        dd = i.as_dict()
        print(json.dumps(dd, indent=4))


@extractor.command()
@click.option('--recursive', is_flag=True, help='Get following/follower/info recursively.', default=False)
@click.option('-u', '--user', default=None)
@click.option('-n', '--name', show_default=True, default='extractor')
@click.option('-i', '--initfile', required=False, default=None, help='List of users to load')
@click.pass_context
def extract(ctx, recursive, user, name, initfile):
    print(locals())
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    dburi = ctx.obj['DBURI']
    utils.extract(wq,
                  recursive=recursive,
                  user=user,
                  dburi=dburi,
                  initfile=initfile,
                  extractor_name=name)

@extractor.command('reset')
@click.pass_context
def reset_extractor(ctx):
    db = ctx.obj['DBURI']
    session = make_session(db)
    session.query(ExtractorEntry).filter(ExtractorEntry.pending==True).update({'pending':False})


@main.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=False),
              help='''Issue a call to an endpoint of the Twitter API.''')
@click.argument('cmd', nargs=1)
@click.option('--tweets', is_flag=True, help='Fetch more tweets using smart pagination. Use --count to control how many tweets to fetch per call, and --max_count to set the number of desired tweets (or -1 to get as many as possible).', type=bool, default=False)
@click.option('--users', is_flag=True, help='Fetch more users using smart pagination. Use --count to control how many users to fetch per call, and --max_count to set the number of desired users (or -1 to get as many as possible).', type=bool, default=False)
@click.argument('api_args', nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def api(ctx, cmd, tweets, users, api_args):
    opts = {}
    mappings = {
        'id': '_id'
    }
    i = iter(api_args)
    for k, v in zip(i, i):
        k = k.replace('--', '')
        if k in mappings:
            k = mappings[k]
        opts[k] = v
    wq = crawlers.TwitterQueue.from_config(conffile=bconf.CONFIG_FILE)
    if tweets:
        resp = utils.consume_tweets(wq[cmd], **opts)
    elif users:
        resp = utils.consume_users(wq[cmd], **opts)
    else:
        resp = wq[cmd](**opts)
        print(json.dumps(resp))
        return
    for i in resp:
        print(json.dumps(i))


@main.command('server')
@click.argument('CONSUMER_KEY', required=True)
@click.argument('CONSUMER_SECRET', required=True)
@click.pass_context
def run_server(ctx, consumer_key, consumer_secret):
    bconf.CONSUMER_KEY = consumer_key
    bconf.CONSUMER_SECRET = consumer_secret
    from .webserver import app
    app.run(host='0.0.0.0')

@main.group()
@click.pass_context
def stream(ctx):
    pass

@stream.command('get')
@click.option('-l', '--locations', default=None)
@click.option('-t', '--track', default=None)
@click.option('-f', '--file', default=None, help='File to store the stream of tweets')
@click.option('-p', '--politelyretry', help='Politely retry after a hangup/connection error', is_flag=True, default=True)
@click.pass_context
def get_stream(ctx, locations, track, file, politelyretry):
    wq = crawlers.StreamQueue.from_config(conffile=bconf.CONFIG_FILE, max_workers=1)

    query_args = {}
    if locations:
        query_args['locations'] = locations
    if track:
        query_args['track'] = track
    if not file:
        file = sys.stdout
    else:
        file = open(file, 'a')

    def insist():
        lasthangup = time.time()
        while True:
            if not query_args:
                iterator = wq.statuses.sample()
            else:
                iterator = wq.statuses.filter(**query_args)#"-4.25,40.16,-3.40,40.75")
            try:
              for i in iterator:
                  yield i
              if not politelyretry:
                  return
            except Exception:
                if not politelyretry:
                    raise ex
            thishangup = time.time()
            if thishangup - lasthangup < 60:
                raise Exception('Too many hangups in a row.')
            time.sleep(3)

    for tweet in tqdm(insist()):
        print(json.dumps(tweet), file=file)
    if file != sys.stdout:
        file.close()

@stream.command('read')
@click.option('-f', '--file', help='File to read the stream of tweets from', required=True)
@click.option('-t', '--tail', is_flag=True, help='Keep reading from the file, like tail', type=bool, default=False)
@click.pass_context
def read_stream(ctx, file, tail):
    for tweet in utils.read_file(file, tail=tail):
        try:
            print(u'{timestamp_ms}- @{screen_name}: {text}'.format(timestamp_ms=tweet['created_at'], screen_name=tweet['user']['screen_name'], text=tweet['text']))
        except (KeyError, TypeError):
            print('Raw tweet: {}'.format(tweet))

@stream.command('tags')
@click.option('-f', '--file', help='File to read the stream of tweets from', required=True)
@click.argument('limit', required=False, default=None, type=int)
@click.pass_context
def tags_stream(ctx, file, limit):
    c = utils.get_hashtags(utils.read_file(file))
    for count, tag in c.most_common(limit):
        print(u'{} - {}'.format(count, tag))


if __name__ == '__main__':
    main()
