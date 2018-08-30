from __future__ import print_function

import logging
import time
import json
import yaml
import csv
import io

import signal
import sys
import sqlalchemy
import os
import multiprocessing
from multiprocessing.pool import ThreadPool

from functools import partial

from tqdm import tqdm

from itertools import islice, chain
from contextlib import contextmanager

from collections import Counter

from builtins import map, filter

from twitter import TwitterHTTPError

from bitter.models import Following, User, ExtractorEntry, make_session

from bitter import config

# Fix Python 2.x.
try:
    UNICODE_EXISTS = bool(type(unicode))
except NameError:
    unicode = lambda s: str(s)

logger = logging.getLogger(__name__)


def signal_handler(signal, frame):
    logger.info('You pressed Ctrl+C!')
    sys.exit(0)


def chunk(iterable, n):
    it = iter(iterable)
    return iter(lambda: tuple(islice(it, n)), ())


def parallel(func, source, chunksize=1, numcpus=multiprocessing.cpu_count()):
    source = chunk(source, chunksize)
    p = ThreadPool(numcpus*2)
    results = p.imap_unordered(func, source, chunksize=int(1000/numcpus))
    for i in chain.from_iterable(results):
        yield i


def get_config_path(conf=None):
    if not conf:
        if config.CONFIG_FILE:
            conf = config.CONFIG_FILE
        else:
            raise Exception('No valid config file')
    return os.path.expanduser(conf)


def copy_credentials_to_config(credfile, conffile=None):
      p = get_config_path(credfile)
      with open(p) as old:
          for line in old:
              cred = json.loads(line.strip())
              add_credentials(conffile, **cred)


def save_config(conf, conffile=None):
    with config(conffile) as c:
        c.clear()
        c.update(conf)


@contextmanager
def config(conffile=None):
    d = read_config(conffile)
    try:
        yield d
    finally:
        write_config(d, conffile)


def read_config(conffile):
    p = conffile and get_config_path(conffile)
    if p:
        if not os.path.exists(p):
            raise IOError('{} file does not exist.'.format(p))
        f = open(p, 'r')
    elif 'BITTER_CONFIG' not in os.environ:
        raise Exception('No config file or BITTER_CONFIG env variable.')
    else:
        f = io.StringIO(unicode(os.environ.get('BITTER_CONFIG', "")).strip().replace('\\n', '\n'))
    return yaml.load(f) or {'credentials': []}


def write_config(conf, conffile=None):
    if not conf:
        conf = {'credentials': []}
    if conffile:
        p = get_config_path(conffile)
        with open(p, 'w') as f:
            yaml.dump(conf, f)
    else:
        os.environ['BITTER_CONFIG'] = yaml.dump(conf)

def iter_credentials(conffile=None):
    with config(conffile) as c:
        for i in c['credentials']:
            yield i


def create_config_file(conffile=None):
    if not conffile:
        return
    conffile = get_config_path(conffile)
    with open(conffile, 'a'):
        pass
    write_config(None, conffile)


def get_credentials(conffile=None, inverse=False, **kwargs):
    creds = []
    for i in iter_credentials(conffile):
        matches = all(map(lambda x: i[x[0]] == x[1], kwargs.items()))
        if matches and not inverse:
            creds.append(i)
        elif inverse and not matches:
            creds.append(i)
    return creds


def delete_credentials(conffile=None, **creds):
    tokeep = get_credentials(conffile, inverse=True, **creds)
    with config(conffile) as c:
        c['credentials'] = list(tokeep)


def add_credentials(conffile=None, **creds):
    try:
        exist = get_credentials(conffile, **creds)
    except IOError:
        exist = False
        create_config_file(conffile)
    if exist:
        return
    with config(conffile) as c:
        c['credentials'].append(creds)


def get_hashtags(iter_tweets, best=None):
    c = Counter()
    for tweet in iter_tweets:
        c.update(tag['text'] for tag in tweet.get('entities', {}).get('hashtags', {}))
    return c


def read_file(filename, tail=False):
    if filename == '-':
        f = sys.stdin
    else:
        f = open(filename)
    try:
      while True:
          line = f.readline()
          if line not in (None, '', '\n'):
              tweet = json.loads(line.strip())
              yield tweet
          else:
              if tail:
                  time.sleep(1)
              else:
                  return
    finally:
        if f != sys.stdin:
          close(f)


def get_users(wq, ulist, by_name=False, queue=None, max_users=100):
    t = 'name' if by_name else 'uid'
    logger.debug('Getting users by {}: {}'.format(t, ulist))
    ilist = iter(ulist)
    while True:
        userslice = ",".join(str(i) for i in islice(ilist, max_users))
        if not userslice:
            break
        try:
            if by_name:
                resp = wq.users.lookup(screen_name=userslice)
            else:
                resp = wq.users.lookup(user_id=userslice)
        except TwitterHTTPError as ex:
            if ex.e.code in (404,):
                resp = []
            else:
                raise
        if not resp:
            logger.debug('Empty response')
        for user in resp:
            user = trim_user(user)
            if queue:
                queue.put(user)
            else:
                yield user


def trim_user(user):
    if 'status' in user:
        del user['status']
    if 'follow_request_sent' in user:
        del user['follow_request_sent']
    if 'created_at' in user:
        ts = time.strftime('%s', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        user['created_at_stamp'] = ts
        del user['created_at']
    user['entities'] = json.dumps(user['entities'])
    return user


def add_user(user, dburi=None, session=None, update=False):
    if not session:
        session = make_session(dburi)

    user = trim_user(user)
    olduser = session.query(User).filter(User.id == user['id'])
    if olduser:
        if not update:
            return
        olduser.delete()
    nuser = User()
    for key, value in user.items():
        setattr(nuser, key, value)
    user = nuser
    if update:
        session.add(user)
        logger.debug('Adding entry')
        entry = session.query(ExtractorEntry).filter(ExtractorEntry.user==user.id).first()
        if not entry:
            entry = ExtractorEntry(user=user.id)
            session.add(entry)
        logger.debug(entry.pending)
        entry.pending = True
        entry.cursor = -1
        session.commit()
    session.close()


def download_entry(wq, entry_id, dburi=None, recursive=False):
    session = make_session(dburi)
    if not session:
        raise Exception("Provide dburi or session")
    logger.info("Downloading entry: %s (%s)" % (entry_id, type(entry_id)))
    entry = session.query(ExtractorEntry).filter(ExtractorEntry.id==entry_id).first()
    user = session.query(User).filter(User.id == entry.user).first()
    download_user(wq, session, user, entry, recursive)
    session.close()


def download_user(wq, session, user, entry=None, recursive=False, max_followers=50000):

    total_followers = user.followers_count

    if total_followers > max_followers:
        entry.pending = False
        logger.info("Too many followers for user: %s" % user.screen_name)
        session.add(entry)
        session.commit()
        return

    if not entry:
        entry = session.query(ExtractorEntry).filter(ExtractorEntry.user==user.id).first() or ExtractorEntry(user=user.id)
    session.add(entry)
    session.commit()

    pending = True
    cursor = entry.cursor
    uid = user.id
    name = user.name

    logger.info("#"*20)
    logger.info("Getting %s - %s" % (uid, name))
    logger.info("Cursor %s" % cursor)
    logger.info("Using account: %s" % wq.name)

    _fetched_followers = 0

    def fetched_followers():
        return session.query(Following).filter(Following.isfollowed==uid).count()

    attempts = 0
    while cursor > 0 or fetched_followers() < total_followers:
        try:
            resp = wq.followers.ids(user_id=uid, cursor=cursor)
        except TwitterHTTPError as ex:
            attempts += 1
            if ex.e.code in (401, ) or attempts > 3:
                logger.info('Not authorized for user: {}'.format(uid))
                entry.errors = ex.message
                break
        if 'ids' not in resp:
            logger.info("Error with id %s %s" % (uid, resp))
            entry.pending = False
            entry.errors = "No ids in response: %s" % resp
            break

        logger.info("New followers: %s" % len(resp['ids']))
        if recursive:
            newusers = get_users(wq, resp)
            for newuser in newusers:
                add_user(session=session, user=newuser)

        if 'ids' not in resp or not resp['ids']:
            logger.info('NO IDS in response')
            break
        for i in resp['ids']:
            existing_user = session.query(Following).\
                            filter(Following.isfollowed == uid).\
                            filter(Following.follower == i).first()
            now = int(time.time())
            if existing_user:
                existing_user.created_at_stamp = now
            else:
                f = Following(isfollowed=uid,
                              follower=i,
                              created_at_stamp=now)
                session.add(f)

        logger.info("Fetched: %s/%s followers" % (fetched_followers(),
                                                  total_followers))
        entry.cursor = resp["next_cursor"]

        session.add(entry)
        session.commit()

    logger.info("Done getting followers for %s" % uid)

    entry.pending = False
    entry.busy = False
    session.add(entry)
    session.commit()

    logger.debug('Entry: {} - {}'.format(entry.user, entry.pending))
    sys.stdout.flush()


def classify_user(id_or_name, screen_names, user_ids):
    try:
        int(id_or_name)
        user_ids.append(id_or_name)
        logger.debug("Added user id")
    except ValueError:
        logger.debug("Added screen_name")
        screen_names.append(id_or_name.split('@')[-1])


def extract(wq, recursive=False, user=None, initfile=None, dburi=None, extractor_name=None):
    signal.signal(signal.SIGINT, signal_handler)

    if not dburi:
        dburi = 'sqlite:///%s.db' % extractor_name

    session = make_session(dburi)
    session.query(ExtractorEntry).update({ExtractorEntry.busy: False})
    session.commit()


    if not (user or initfile):
        logger.info('Using pending users from last session')
    else:
        screen_names = []
        user_ids = []
        if user:
            classify_user(user, screen_names, user_ids)
        elif initfile:
            logger.info("No user. I will open %s" % initfile)
            with open(initfile, 'r') as f:
                for line in f:
                    user = line.strip().split(',')[0]
                    classify_user(user, screen_names, user_ids)

        def missing_user(ix, column=User.screen_name):
            res = session.query(User).filter(column == ix).count() == 0
            if res:
                logger.info("Missing user %s. Count: %s" % (ix, res))
            return res

        screen_names = list(filter(missing_user, screen_names))
        user_ids = list(filter(partial(missing_user, column=User.id_str), user_ids))
        nusers = []
        logger.info("Missing user ids: %s" % user_ids)
        logger.info("Missing screen names: %s" % screen_names)
        if screen_names:
            nusers = list(get_users(wq, screen_names, by_name=True))
        if user_ids:
            nusers += list(get_users(wq, user_ids, by_name=False))

        for i in nusers:
            add_user(dburi=dburi, user=i)

    total_users = session.query(sqlalchemy.func.count(User.id)).scalar()
    logger.info('Total users: {}'.format(total_users))

    de = partial(download_entry, wq, dburi=dburi)
    pending = pending_entries(dburi)
    session.close()

    for i in tqdm(parallel(de, pending), desc='Downloading users', total=total_users):
        logger.info("Got %s" % i)


def pending_entries(dburi):
    session = make_session(dburi)
    while True:
        candidate, entry = session.query(User, ExtractorEntry).\
                        filter(ExtractorEntry.user == User.id).\
                        filter(ExtractorEntry.pending == True).\
                        filter(ExtractorEntry.busy == False).\
                        order_by(User.followers_count).first()
        if candidate:
            entry.busy = True
            session.add(entry)
            session.commit()
            yield int(entry.id)
            continue
        if session.query(ExtractorEntry).\
            filter(ExtractorEntry.busy == True).count() > 0:
            time.sleep(1)
            continue
        logger.info("No more pending entries")
        break
    session.close()

def get_tweet(c, tid):
    return c.statuses.show(id=tid)

def search_tweet(c, query):
    return c.search.tweets(q=query)

def user_timeline(c, query):
    try:
        return c.statuses.user_timeline(user_id=int(query))
    except ValueError:
        return c.statuses.user_timeline(screen_name=query)

def get_user(c, user):
    try:
        int(user)
        return c.users.lookup(user_id=user)[0]
    except ValueError:
        return c.users.lookup(screen_name=user)[0]

def download_tweet(wq, tweetid, write=True, folder="downloaded_tweets", update=False):
    cached = cached_id(tweetid, folder)
    tweet = None
    if update or not cached:
        tweet = get_tweet(wq, tweetid)
        js = json.dumps(tweet)
    if write:
        if tweet:
            write_json(js, folder)
    else:
        print(js)


def cached_id(oid, folder):
    tweet = None
    file = os.path.join(folder, '%s.json' % oid)
    if os.path.exists(file) and os.path.isfile(file):
        try:
            # print('%s: Object exists' % oid)
            with open(file) as f:
                tweet = json.load(f)
        except Exception as ex:
            logger.error('Error getting cached version of {}: {}'.format(oid, ex))
    return tweet

def write_json(js, folder, oid=None):
    if not oid:
      oid = js['id']
    file = id_file(oid, folder)
    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(file, 'w') as f:
        json.dump(js, f)
        logger.info('Written {} to file {}'.format(oid, file))

def id_file(oid, folder):
    return os.path.join(folder, '%s.json' % oid)

def fail_file(oid, folder):
    failsfolder = os.path.join(folder, 'failed')
    if not os.path.exists(failsfolder):
        os.makedirs(failsfolder)
    return os.path.join(failsfolder, '%s.failed' % oid)

def id_failed(oid, folder):
    return os.path.isfile(fail_file(oid, folder))

def tweet_download_batch(wq, batch):
    tweets = wq.statuses.lookup(_id=",".join(batch), map=True)['id']
    return tweets.items()

def user_download_batch(wq, batch):
    screen_names = []
    user_ids = []
    for elem in batch:
        try:
            int(elem)
            user_ids.append(str(elem))
        except ValueError:
            screen_names.append(elem.lower())
    print('Downloading: {} - {}'.format(user_ids, screen_names))
    users = wq.users.lookup(user_id=",".join(user_ids), screen_name=",".join(screen_names))
    found_ids = []
    found_names = []
    for user in users:
        uid = user['id']
        if uid in user_ids:
            found_ids.append(uid)
            yield (uid, user)
        uname = user['screen_name'].lower()
        if uname in screen_names:
            found_names.append(uname)
            yield (uname, user)
    for uid in set(user_ids) - set(found_ids):
        yield (uid, None)
    for name in set(screen_names) - set(found_names):
        yield (name, None)


def download_list(wq, lst, folder, update=False, retry_failed=False, ignore_fails=True,
                  batch_method=tweet_download_batch):
    def filter_lines(line):
        # print('Checking {}'.format(line))
        oid = line[0]
        if (cached_id(oid, folder) and not update) or (id_failed(oid, folder) and not retry_failed):
            yield None
        else:
            yield str(oid)

    def print_result(res):
        for oid, obj in res:
          if obj:
              try:
                  write_json(obj, folder=folder, oid=oid)
                  yield 1
              except Exception as ex:
                  logger.error('%s: %s' % (oid, ex))
                  if not ignore_fails:
                      raise
          else:
              logger.info('Object not recovered: {}'.format(oid))
              with open(fail_file(oid, folder), 'w') as f:
                  print('Object not found', file=f)
              yield -1

    objects_to_crawl = filter(lambda x: x is not None, tqdm(parallel(filter_lines, lst), desc='Total objects'))
    batch_method = partial(batch_method, wq)
    tweets = parallel(batch_method, objects_to_crawl, 100)
    for res in tqdm(parallel(print_result, tweets), desc='Queried'):
        yield res


def download_file(wq, csvfile, folder, column=0, delimiter=',',
                  header=False, quotechar='"', batch_method=tweet_download_batch,
                  **kwargs):
    with open(csvfile) as f:
        csvreader = csv.reader(f, delimiter=str(delimiter), quotechar=str(quotechar))
        if header:
            next(csvreader)
        tweets = map(lambda row: row[0].strip(), csvreader)
        for res in download_list(wq, tweets, folder, batch_method=batch_method,
                                 **kwargs):
            yield res


def download_timeline(wq, user):
    return wq.statuses.user_timeline(id=user)


def _consume_feed(func, feed_control=None, **kwargs):
    '''
    Get all the tweets using pagination and a given method.
    It can be controlled with the `count` parameter.

    If max_count < 0 => Loop until the whole feed is consumed.
    If max_count == 0 => Only call the API once, with the default values.
    If max_count > 0 => Get max_count tweets from the feed.
    '''
    remaining = int(kwargs.pop('max_count', 0))
    count = int(kwargs.get('count', -1))
    limit = False

    # We need to at least perform a query, so we simulate a do-while
    # by running once with no limit and updating the condition at the end
    with tqdm(total=remaining) as pbar:
      while not limit:
          if remaining > 0 and  ((count < 0) or (count > remaining)):
              kwargs['count'] = remaining
          resp, stop = feed_control(func, kwargs, remaining=remaining, batch_size=count)
          if not resp:
              return
          for entry in resp:
              yield entry
          pbar.update(len(resp))
          limit = stop
          if remaining < 0:
              # If the loop was run with a negative remaining, it will only stop
              # when the control function tells it to.
              continue
          # Otherwise, check if we have already downloaded all the required items
          remaining -= len(resp)
          limit = limit or remaining <= 0


def consume_tweets(*args, **kwargs):
    return _consume_feed(*args, feed_control=_tweets_control, **kwargs)


def consume_users(*args, **kwargs):
    return _consume_feed(*args, feed_control=_users_control, **kwargs)


def _tweets_control(func, apiargs, remaining=0, **kwargs):
    ''' Return a list of entries, the remaining '''
    
    resp = func(**apiargs)
    if not resp:
        return None, True
    # Update the arguments for the next call
    # Two options: either resp is a list, or a dict like:
    #    {'statuses': ... 'search_metadata': ...}
    if isinstance(resp, dict) and 'search_metadata' in resp:
        resp = resp['statuses']
    max_id = min(s['id'] for s in resp) - 1
    apiargs['max_id'] = max_id
    return resp, False


def _users_control(func, apiargs, remaining=0, **kwargs):
    resp = func(**apiargs)
    stop = True
    # Update the arguments for the next call
    if 'next_cursor' in resp:
        cursor = resp['next_cursor']
        apiargs['cursor'] = cursor
        if int(cursor) != -1:
            stop = False
    return resp['users'], stop
