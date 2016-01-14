import click
import json
import logging
import time
import sqlalchemy.types
import threading
import sqlite3

from six.moves import map, filter, queue
from sqlalchemy import exists

from bitter import utils, models, crawlers
from bitter.models import make_session, User, ExtractorEntry, Following
from contextlib import ExitStack

logger = logging.getLogger(__name__)

@click.group()
@click.option("--verbose", is_flag=True)
@click.option("--logging_level", required=False, default='WARN')
@click.option("--config", required=False)
@click.option('-c', '--credentials',show_default=True, default='credentials.json')
@click.pass_context
def main(ctx, verbose, logging_level, config, credentials):
    logging.basicConfig(level=getattr(logging, logging_level))
    ctx.obj = {}
    ctx.obj['VERBOSE'] = verbose
    ctx.obj['CONFIG'] = config
    ctx.obj['CREDENTIALS'] = credentials


@main.group()
@click.pass_context 
def tweet(ctx):
    pass

@tweet.command('get')
@click.argument('tweetid')
@click.pass_context 
def get_tweet(ctx, tweetid):
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
    c = wq.next()
    t = crawlers.get_tweet(c.client, tweetid)
    print(json.dumps(t, indent=2))
        

@tweet.command('search')
@click.argument('query')
@click.pass_context 
def get_tweet(ctx, query):
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
    c = wq.next()
    t = utils.search_tweet(c.client, query)
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

@users.command('get_one')
@click.argument('user')
@click.pass_context 
def get_user(ctx, user):
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
    c = wq.next()
    u = utils.get_user(c.client, user)
    print(json.dumps(u, indent=2))

@users.command('get')
@click.option('--db', required=True, help='Database to save all users.')
@click.option('--skip', required=False, default=0, help='Skip N lines from the file.')
@click.option('--until', required=False, type=str, default=0, help='Skip all lines until ID.')
@click.option('--threads', required=False, type=str, default=20, help='Number of crawling threads.')
@click.argument('usersfile', 'File with a list of users to look up')
@click.pass_context
def get_users(ctx, usersfile, skip, until, threads, db):
    global dburl, ids_queue, skipped, enqueued, collected, lastid, db_lock

    if '://' not in db:
        dburl = 'sqlite:///{}'.format(db)
        db_lock = threading.Lock()
    else:
        dburl = db
        def db_lock():
            return ExitStack()


    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
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
            user['entities'] = json.dumps(user['entities'])
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

@main.group('api')
def api():
    pass


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


@extractor.command()
@click.option('--recursive', is_flag=True, help='Get following/follower/info recursively.', default=False)
@click.option('-u', '--user', default=None)
@click.option('-n', '--name', show_default=True, default='extractor')
@click.option('-i', '--initfile', required=False, default=None, help='List of users to load')
@click.pass_context
def extract(ctx, recursive, user, name, initfile):
    print(locals())
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
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
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
    db = ctx.obj['DBURI']
    session = make_session(db)
    session.query(ExtractorEntry).filter(ExtractorEntry.pending==True).update({'pending':False})



@api.command('limits')
@click.argument('url', required=False)
@click.pass_context
def get_limits(ctx, url):
    wq = crawlers.TwitterQueue.from_credentials(ctx.obj['CREDENTIALS'])
    for worker in wq.queue:
        resp = worker.client.application.rate_limit_status()
        print('#'*20)
        print(worker.name)
        if url:
            limit = 'NOT FOUND'
            print('URL is: {}'.format(url))
            cat = url.split('/')[1]
            if cat in resp['resources']:
                limit = resp['resources'][cat].get(url, None) or resp['resources'][cat]
            else:
                print('Cat {} not found'.format(cat))
            print('{}: {}'.format(url, limit))           
        else:
            print(json.dumps(resp, indent=2))

if __name__ == '__main__':
    main()
